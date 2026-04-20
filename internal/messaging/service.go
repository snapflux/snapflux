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

	"snapflux/api-service-go/internal/broker"
	"snapflux/api-service-go/internal/config"
	"snapflux/api-service-go/internal/model"
	"snapflux/api-service-go/internal/storage"
	"snapflux/api-service-go/internal/wal"
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
}

func NewService(store storage.Provider, brokerSvc *broker.Broker, batch *BatchFlush, walSvc *wal.WAL, cfg *config.Config) *Service {
	return &Service{
		store:      store,
		broker:     brokerSvc,
		batch:      batch,
		walSvc:     walSvc,
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *Service) Send(ctx context.Context, topic, key string, body any, forwarded bool, durability model.Durability) (model.SendResponse, error) {
	if durability == "" {
		durability = model.DurabilityDurable
	}

	if !forwarded && !s.broker.IsSelf(key) {
		target, err := s.broker.GetNode(key)
		if err != nil {
			return model.SendResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		slog.Debug("forwarding send", "topic", topic, "key", key, "target", target.Address)
		return s.forwardSend(ctx, target, topic, model.SendRequest{Key: key, Body: body, Durability: durability})
	}

	msgID := rand.Text()

	if durability == model.DurabilityStrict {
		if err := s.store.SendMessage(ctx, msgID, topic, key, body); err != nil {
			return model.SendResponse{}, err
		}
		groups, err := s.store.GetSubscriptions(ctx, topic)
		if err != nil {
			return model.SendResponse{}, err
		}
		if len(groups) > 0 {
			if err := s.store.CreateDeliveries(ctx, msgID, topic, groups, s.cfg.MaxDeliveryAttempts); err != nil {
				return model.SendResponse{}, err
			}
		}
		return model.SendResponse{ID: msgID, Durability: durability}, nil
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	if durability == model.DurabilityFireAndForget {
		s.batch.Enqueue(nil, msgID, topic, key, body, ts)
		return model.SendResponse{ID: msgID, Durability: durability}, nil
	}

	// durable: write to WAL first
	walEntry, err := s.walSvc.Append(wal.Entry{
		ID: msgID, Topic: topic, Key: key, Payload: body,
		Durability: string(durability), Timestamp: ts,
	})
	if err != nil {
		if errors.Is(err, wal.ErrBackpressure) {
			slog.Warn("WAL backpressure — rejecting send", "topic", topic, "key", key)
			return model.SendResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		return model.SendResponse{}, err
	}
	s.batch.Enqueue(&walEntry, msgID, topic, key, body, ts)
	return model.SendResponse{ID: msgID, Durability: durability}, nil
}

func (s *Service) Receive(ctx context.Context, topic, group string, limit int, forwarded bool) ([]model.MessageResponse, error) {
	routeKey := topic + ":" + group
	if !forwarded && !s.broker.IsSelf(routeKey) {
		target, err := s.broker.GetNode(routeKey)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		slog.Debug("forwarding receive", "topic", topic, "group", group, "target", target.Address)
		return s.forwardReceive(ctx, target, topic, group, limit)
	}

	if err := s.store.UpsertSubscription(ctx, topic, group); err != nil {
		return nil, err
	}

	deliveries, err := s.store.ReceiveDeliveries(ctx, topic, group, limit, s.cfg.VisibilityTimeoutMs)
	if err != nil {
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
	return result, nil
}

func (s *Service) Ack(ctx context.Context, topic, group, receiptID string, forwarded bool) (model.AckResponse, error) {
	routeKey := topic + ":" + group
	if !forwarded && !s.broker.IsSelf(routeKey) {
		target, err := s.broker.GetNode(routeKey)
		if err != nil {
			return model.AckResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
		}
		slog.Debug("forwarding ack", "topic", topic, "group", group, "receiptId", receiptID, "target", target.Address)
		return s.forwardAck(ctx, target, topic, group, receiptID)
	}

	ok, err := s.store.AckDelivery(ctx, receiptID)
	if err != nil {
		return model.AckResponse{}, err
	}
	if !ok {
		return model.AckResponse{}, fmt.Errorf("%w: receipt not found or already acknowledged", ErrNotFound)
	}
	return model.AckResponse{ReceiptID: receiptID, Acknowledged: true}, nil
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

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		slog.Error("forward send failed", "topic", topic, "target", target.Address, "error", err)
		return model.SendResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
	}
	defer resp.Body.Close()
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

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		slog.Error("forward receive failed", "topic", topic, "group", group, "target", target.Address, "error", err)
		return nil, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
	}
	defer resp.Body.Close()
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

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		slog.Error("forward ack failed", "topic", topic, "group", group, "receiptId", receiptID, "target", target.Address, "error", err)
		return model.AckResponse{}, fmt.Errorf("%w: %v", ErrServiceUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Error("forward ack upstream error", "topic", topic, "group", group, "receiptId", receiptID, "target", target.Address, "status", resp.StatusCode)
		return model.AckResponse{}, fmt.Errorf("%w: upstream returned %d", ErrServiceUnavailable, resp.StatusCode)
	}
	var result model.AckResponse
	return result, json.NewDecoder(resp.Body).Decode(&result)
}
