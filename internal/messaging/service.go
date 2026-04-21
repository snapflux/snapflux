package messaging

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"vinr.eu/snapflux/internal/broker"
	"vinr.eu/snapflux/internal/config"
	"vinr.eu/snapflux/internal/model"
	"vinr.eu/snapflux/internal/storage"
	"vinr.eu/snapflux/internal/wal"
)

var (
	ErrNotFound           = errors.New("not found")
	ErrServiceUnavailable = errors.New("service unavailable")
)

type Service struct {
	store      storage.Provider
	broker     *broker.Broker
	batch      *BatchFlush
	walSvc     *wal.WAL
	cfg        *config.Config
	httpClient *http.Client
	tracer     trace.Tracer
	sent       metric.Int64Counter
	received   metric.Int64Counter
	acked      metric.Int64Counter
	forwarded  metric.Int64Counter
}

func NewService(store storage.Provider, brokerSvc *broker.Broker, batch *BatchFlush, walSvc *wal.WAL, cfg *config.Config) *Service {
	meter := otel.GetMeterProvider().Meter("snapflux/messaging")
	s := &Service{
		store:      store,
		broker:     brokerSvc,
		batch:      batch,
		walSvc:     walSvc,
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		tracer:     otel.GetTracerProvider().Tracer("snapflux/messaging"),
	}
	s.sent, _ = meter.Int64Counter("snapflux.messages.sent",
		metric.WithDescription("Total messages sent"))
	s.received, _ = meter.Int64Counter("snapflux.messages.received",
		metric.WithDescription("Total messages received"))
	s.acked, _ = meter.Int64Counter("snapflux.messages.acked",
		metric.WithDescription("Total messages acknowledged"))
	s.forwarded, _ = meter.Int64Counter("snapflux.messages.forwarded",
		metric.WithDescription("Total messages forwarded to peer brokers"))
	return s
}

func (s *Service) Send(ctx context.Context, topic, key string, body any, forwarded bool, durability model.Durability) (model.SendResponse, error) {
	ctx, span := s.tracer.Start(ctx, "snapflux.send",
		trace.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("durability", string(durability)),
		),
	)
	defer span.End()

	if durability == "" {
		durability = model.DurabilityDurable
	}

	if !forwarded && !s.broker.IsSelf(key) {
		target, err := s.broker.GetNode(key)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return model.SendResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		slog.Debug("forwarding send", "topic", topic, "key", key, "target", target.Address)
		s.forwarded.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "send")))
		return forwardWithRetry(func() (model.SendResponse, error) {
			return s.forwardSend(ctx, target, topic, model.SendRequest{Key: key, Body: body, Durability: durability})
		})
	}

	msgID := rand.Text()

	if durability == model.DurabilityStrict {
		if err := s.store.SendMessage(ctx, msgID, topic, key, body); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return model.SendResponse{}, err
		}
		groups, err := s.store.GetSubscriptions(ctx, topic)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return model.SendResponse{}, err
		}
		if len(groups) > 0 {
			if err := s.store.CreateDeliveries(ctx, msgID, topic, groups, s.cfg.MaxDeliveryAttempts); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return model.SendResponse{}, err
			}
		}
		s.sent.Add(ctx, 1, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("durability", string(durability)),
		))
		return model.SendResponse{ID: msgID, Durability: durability}, nil
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	if durability == model.DurabilityFireAndForget {
		s.batch.Enqueue(nil, msgID, topic, key, body, ts)
		s.sent.Add(ctx, 1, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("durability", string(durability)),
		))
		return model.SendResponse{ID: msgID, Durability: durability}, nil
	}

	// durable: write to WAL first
	walEntry, err := s.walSvc.Append(wal.Entry{
		ID: msgID, Topic: topic, Key: key, Payload: body,
		Durability: string(durability), Timestamp: ts,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if errors.Is(err, wal.ErrBackpressure) {
			slog.Warn("WAL backpressure — rejecting send", "topic", topic, "key", key)
			return model.SendResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		return model.SendResponse{}, err
	}
	s.batch.Enqueue(&walEntry, msgID, topic, key, body, ts)
	s.sent.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("durability", string(durability)),
	))
	return model.SendResponse{ID: msgID, Durability: durability}, nil
}

func (s *Service) Receive(ctx context.Context, topic, group string, limit int, forwarded bool) ([]model.MessageResponse, error) {
	ctx, span := s.tracer.Start(ctx, "snapflux.receive",
		trace.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("group", group),
		),
	)
	defer span.End()

	routeKey := topic + ":" + group
	if !forwarded && !s.broker.IsSelf(routeKey) {
		target, err := s.broker.GetNode(routeKey)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		slog.Debug("forwarding receive", "topic", topic, "group", group, "target", target.Address)
		s.forwarded.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "receive")))
		return forwardWithRetry(func() ([]model.MessageResponse, error) {
			return s.forwardReceive(ctx, target, topic, group, limit)
		})
	}

	if err := s.store.UpsertSubscription(ctx, topic, group); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	deliveries, err := s.store.ReceiveDeliveries(ctx, topic, group, limit, s.cfg.VisibilityTimeoutMs)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	result := make([]model.MessageResponse, len(deliveries))
	for i, d := range deliveries {
		result[i] = model.MessageResponse{
			ID:        d.MessageID,
			ReceiptID: d.ReceiptID,
			Key:       d.Key,
			Body:      d.Body,
			Attempts:  d.Attempts,
			SentAt:    d.SentAt.UTC().Format(time.RFC3339Nano),
		}
	}
	s.received.Add(ctx, int64(len(result)), metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("group", group),
	))
	return result, nil
}

func (s *Service) Ack(ctx context.Context, topic, group, receiptID string, forwarded bool) (model.AckResponse, error) {
	ctx, span := s.tracer.Start(ctx, "snapflux.ack",
		trace.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("group", group),
		),
	)
	defer span.End()

	routeKey := topic + ":" + group
	if !forwarded && !s.broker.IsSelf(routeKey) {
		target, err := s.broker.GetNode(routeKey)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return model.AckResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		slog.Debug("forwarding ack", "topic", topic, "group", group, "receiptId", receiptID, "target", target.Address)
		s.forwarded.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "ack")))
		return forwardWithRetry(func() (model.AckResponse, error) {
			return s.forwardAck(ctx, target, topic, group, receiptID)
		})
	}

	ok, err := s.store.AckDelivery(ctx, receiptID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return model.AckResponse{}, err
	}
	if !ok {
		err = fmt.Errorf("%w: receipt not found or already acknowledged", ErrNotFound)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return model.AckResponse{}, err
	}
	s.acked.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("group", group),
	))
	return model.AckResponse{ReceiptID: receiptID, Acknowledged: true}, nil
}

// forwardRetryDelays controls the back-off between peer forward attempts.
var forwardRetryDelays = []time.Duration{100 * time.Millisecond, 300 * time.Millisecond}

func forwardWithRetry[T any](fn func() (T, error)) (T, error) {
	result, err := fn()
	for i, d := range forwardRetryDelays {
		if err == nil {
			return result, nil
		}
		_ = i
		time.Sleep(d)
		result, err = fn()
	}
	return result, err
}

func (s *Service) forwardSend(ctx context.Context, target storage.BrokerEntity, topic string, req model.SendRequest) (model.SendResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return model.SendResponse{}, err
	}
	targetURL := fmt.Sprintf("%s/v1/topics/%s/messages", target.Address, url.PathEscape(topic))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return model.SendResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Snapflux-Hop", "1")
	if s.cfg.BrokerAuthKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+s.cfg.BrokerAuthKey)
	}

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		slog.Error("forward send failed", "topic", topic, "target", target.Address, "error", err)
		return model.SendResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		slog.Error("forward send upstream error", "topic", topic, "target", target.Address, "status", resp.StatusCode)
		return model.SendResponse{}, fmt.Errorf("%w: upstream returned %d", ErrServiceUnavailable, resp.StatusCode)
	}
	var result model.SendResponse
	return result, json.NewDecoder(resp.Body).Decode(&result)
}

func (s *Service) forwardReceive(ctx context.Context, target storage.BrokerEntity, topic, group string, limit int) ([]model.MessageResponse, error) {
	targetURL := fmt.Sprintf("%s/v1/topics/%s/messages?group=%s&limit=%d",
		target.Address, url.PathEscape(topic), url.QueryEscape(group), limit)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("X-Snapflux-Hop", "1")
	if s.cfg.BrokerAuthKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+s.cfg.BrokerAuthKey)
	}

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		slog.Error("forward receive failed", "topic", topic, "group", group, "target", target.Address, "error", err)
		return nil, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		slog.Error("forward receive upstream error", "topic", topic, "group", group, "target", target.Address, "status", resp.StatusCode)
		return nil, fmt.Errorf("%w: upstream returned %d", ErrServiceUnavailable, resp.StatusCode)
	}
	var result []model.MessageResponse
	return result, json.NewDecoder(resp.Body).Decode(&result)
}

func (s *Service) forwardAck(ctx context.Context, target storage.BrokerEntity, topic, group, receiptID string) (model.AckResponse, error) {
	targetURL := fmt.Sprintf("%s/v1/topics/%s/messages/%s?group=%s",
		target.Address, url.PathEscape(topic), url.PathEscape(receiptID), url.QueryEscape(group))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, targetURL, nil)
	if err != nil {
		return model.AckResponse{}, err
	}
	httpReq.Header.Set("X-Snapflux-Hop", "1")
	if s.cfg.BrokerAuthKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+s.cfg.BrokerAuthKey)
	}

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		slog.Error("forward ack failed", "topic", topic, "group", group, "receiptId", receiptID, "target", target.Address, "error", err)
		return model.AckResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		slog.Error("forward ack upstream error", "topic", topic, "group", group, "receiptId", receiptID, "target", target.Address, "status", resp.StatusCode)
		return model.AckResponse{}, fmt.Errorf("%w: upstream returned %d", ErrServiceUnavailable, resp.StatusCode)
	}
	var result model.AckResponse
	return result, json.NewDecoder(resp.Body).Decode(&result)
}
