package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"vinr.eu/snapflux/internal/auth"
	"vinr.eu/snapflux/internal/broker"
	"vinr.eu/snapflux/internal/config"
	"vinr.eu/snapflux/internal/messaging"
	"vinr.eu/snapflux/internal/model"
	"vinr.eu/snapflux/internal/storage"
	"vinr.eu/snapflux/internal/wal"
)

type Server struct {
	srv    *http.Server
	msgSvc *messaging.Service
	store  storage.Provider
	broker *broker.Broker
	walSvc *wal.WAL
}

func New(msgSvc *messaging.Service, store storage.Provider, brokerSvc *broker.Broker, walSvc *wal.WAL, cfg *config.Config, metricsHandler http.Handler) *Server {
	s := &Server{msgSvc: msgSvc, store: store, broker: brokerSvc, walSvc: walSvc}

	guard := auth.New(cfg.AuthKeys, cfg.BrokerAuthKey).Protect

	mux := http.NewServeMux()
	mux.Handle("POST /v1/topics/{topic}/messages",
		otelhttp.NewHandler(guard(http.HandlerFunc(s.handleSend)), "POST /v1/topics/{topic}/messages"))
	mux.Handle("GET /v1/topics/{topic}/messages",
		otelhttp.NewHandler(guard(http.HandlerFunc(s.handleReceive)), "GET /v1/topics/{topic}/messages"))
	mux.Handle("DELETE /v1/topics/{topic}/messages/{receiptId}",
		otelhttp.NewHandler(guard(http.HandlerFunc(s.handleAck)), "DELETE /v1/topics/{topic}/messages/{receiptId}"))
	mux.Handle("GET /health",
		otelhttp.NewHandler(http.HandlerFunc(s.handleHealth), "GET /health"))
	mux.Handle("GET /metrics", metricsHandler)

	s.srv = &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return s
}

func (s *Server) Handler() http.Handler { return s.srv.Handler }

func (s *Server) Start() error {
	slog.Info("server listening", "addr", s.srv.Addr)
	if err := s.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	forwarded := r.Header.Get("X-Snapflux-Hop") != ""

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB
	var req model.SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	slog.Info("send", "topic", topic, "key", req.Key, "durability", req.Durability, "forwarded", forwarded)
	resp, err := s.msgSvc.Send(r.Context(), topic, req.Key, req.Body, forwarded, req.Durability)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	slog.Info("sent", "topic", topic, "msgId", resp.ID, "durability", resp.Durability)
	statusCode := http.StatusOK
	if resp.Durability == model.DurabilityFireAndForget {
		statusCode = http.StatusAccepted
	}
	writeJSON(w, statusCode, resp)
}

func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	forwarded := r.Header.Get("X-Snapflux-Hop") != ""

	group := r.URL.Query().Get("group")
	if group == "" {
		writeError(w, http.StatusBadRequest, "group is required")
		return
	}

	limit := 10
	if ls := r.URL.Query().Get("limit"); ls != "" {
		if n, err := strconv.Atoi(ls); err == nil && n > 0 {
			if n > 100 {
				n = 100
			}
			limit = n
		}
	}

	slog.Info("receive", "topic", topic, "group", group, "limit", limit, "forwarded", forwarded)
	messages, err := s.msgSvc.Receive(r.Context(), topic, group, limit, forwarded)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if messages == nil {
		messages = []model.MessageResponse{}
	}
	slog.Info("received", "topic", topic, "group", group, "count", len(messages))
	writeJSON(w, http.StatusOK, messages)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	receiptID := r.PathValue("receiptId")
	forwarded := r.Header.Get("X-Snapflux-Hop") != ""

	group := r.URL.Query().Get("group")
	if group == "" {
		writeError(w, http.StatusBadRequest, "group is required")
		return
	}

	slog.Info("ack", "topic", topic, "group", group, "receiptId", receiptID, "forwarded", forwarded)
	resp, err := s.msgSvc.Ack(r.Context(), topic, group, receiptID, forwarded)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	slog.Info("acked", "topic", topic, "group", group, "receiptId", receiptID)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]string)
	status := "ok"

	if err := s.store.Ping(r.Context()); err != nil {
		checks["storage"] = "down: " + err.Error()
		status = "error"
	} else {
		checks["storage"] = "up"
	}

	if s.walSvc.IsAccepting() {
		checks["wal"] = "accepting"
	} else {
		checks["wal"] = "backpressure"
		status = "error"
	}

	nodeCount := s.broker.NodeCount()
	if nodeCount > 0 {
		checks["ring"] = strconv.Itoa(nodeCount) + " node(s)"
	} else {
		checks["ring"] = "empty"
		status = "error"
	}

	code := http.StatusOK
	if status == "error" {
		slog.Warn("health degraded", "checks", checks)
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, model.HealthResponse{Status: status, Checks: checks})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, messaging.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, messaging.ErrServiceUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		slog.Error("internal error", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}
