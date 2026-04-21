package memory

import (
	"context"
	"sync"
	"time"

	"crypto/rand"

	"vinr.eu/snapflux/internal/storage"
)

type Provider struct {
	mu            sync.Mutex
	brokers       map[string]*storage.BrokerEntity
	messages      map[string]*storage.MessageEntity
	deliveries    map[string]*storage.DeliveryEntity
	subscriptions map[string]map[string]struct{}

	cbMu            sync.Mutex
	insertCallbacks map[int]func()
	nextCbID        int
}

func New() *Provider {
	return &Provider{
		brokers:         make(map[string]*storage.BrokerEntity),
		messages:        make(map[string]*storage.MessageEntity),
		deliveries:      make(map[string]*storage.DeliveryEntity),
		subscriptions:   make(map[string]map[string]struct{}),
		insertCallbacks: make(map[int]func()),
	}
}

func (p *Provider) Connect(_ context.Context, _ string) error { return nil }
func (p *Provider) Close(_ context.Context) error             { return nil }
func (p *Provider) Ping(_ context.Context) error              { return nil }

func (p *Provider) InitBroker(_ context.Context, id, host, address string) error {
	p.mu.Lock()
	p.brokers[id] = &storage.BrokerEntity{ID: id, Host: host, Address: address, Status: "online", LastHeartbeat: time.Now()}
	p.mu.Unlock()
	return nil
}

func (p *Provider) RemoveBroker(_ context.Context, brokerID string) error {
	p.mu.Lock()
	delete(p.brokers, brokerID)
	p.mu.Unlock()
	return nil
}

func (p *Provider) UpdateHeartbeat(_ context.Context, brokerID string) error {
	p.mu.Lock()
	if b, ok := p.brokers[brokerID]; ok {
		b.LastHeartbeat = time.Now()
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) EvictStaleBrokers(_ context.Context, before time.Time, excludeID string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var evicted []string
	for id, b := range p.brokers {
		if id != excludeID && b.LastHeartbeat.Before(before) {
			delete(p.brokers, id)
			evicted = append(evicted, id)
		}
	}
	return evicted, nil
}

func (p *Provider) GetActiveBrokers(_ context.Context) ([]storage.BrokerEntity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]storage.BrokerEntity, 0, len(p.brokers))
	for _, b := range p.brokers {
		result = append(result, *b)
	}
	return result, nil
}

func (p *Provider) SendMessage(_ context.Context, id, topic, key string, body any) error {
	p.mu.Lock()
	p.messages[id] = &storage.MessageEntity{ID: id, Topic: topic, Key: key, Payload: body, CreatedAt: time.Now()}
	p.mu.Unlock()
	p.emitInsert()
	return nil
}

func (p *Provider) BulkSendMessages(_ context.Context, messages []storage.MessageEntity) error {
	if len(messages) == 0 {
		return nil
	}
	p.mu.Lock()
	for i := range messages {
		m := messages[i]
		p.messages[m.ID] = &m
	}
	p.mu.Unlock()
	p.emitInsert()
	return nil
}

func (p *Provider) BulkCreateDeliveries(_ context.Context, deliveries []storage.DeliveryEntity) error {
	p.mu.Lock()
	for i := range deliveries {
		d := deliveries[i]
		p.deliveries[d.ID] = &d
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) GetSubscriptions(_ context.Context, topic string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]string, 0, len(p.subscriptions[topic]))
	for g := range p.subscriptions[topic] {
		result = append(result, g)
	}
	return result, nil
}

func (p *Provider) UpsertSubscription(_ context.Context, topic, group string) error {
	p.mu.Lock()
	if p.subscriptions[topic] == nil {
		p.subscriptions[topic] = make(map[string]struct{})
	}
	p.subscriptions[topic][group] = struct{}{}
	p.mu.Unlock()
	return nil
}

func (p *Provider) CreateDeliveries(_ context.Context, messageID, topic string, groups []string, maxAttempts int) error {
	now := time.Now()
	p.mu.Lock()
	for _, group := range groups {
		id := rand.Text()
		p.deliveries[id] = &storage.DeliveryEntity{
			ID:          id,
			MessageID:   messageID,
			Topic:       topic,
			Group:       group,
			Status:      storage.StatusPending,
			VisibleAt:   now,
			Attempts:    0,
			MaxAttempts: maxAttempts,
			CreatedAt:   now,
		}
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) ReceiveDeliveries(_ context.Context, topic, group string, limit, visibilityMs int) ([]storage.DeliveryWithMessage, error) {
	now := time.Now()
	visibleAt := now.Add(time.Duration(visibilityMs) * time.Millisecond)

	p.mu.Lock()
	defer p.mu.Unlock()

	var results []storage.DeliveryWithMessage
	for _, d := range p.deliveries {
		if len(results) >= limit {
			break
		}
		if d.Topic != topic || d.Group != group || d.Status != storage.StatusPending || d.VisibleAt.After(now) {
			continue
		}
		msg, ok := p.messages[d.MessageID]
		if !ok {
			continue
		}
		d.Status = storage.StatusInflight
		d.VisibleAt = visibleAt
		d.Attempts++
		results = append(results, storage.DeliveryWithMessage{
			ReceiptID: d.ID,
			MessageID: msg.ID,
			Topic:     msg.Topic,
			Key:       msg.Key,
			Body:      msg.Payload,
			Attempts:  d.Attempts,
			SentAt:    msg.CreatedAt,
		})
	}
	return results, nil
}

func (p *Provider) AckDelivery(_ context.Context, receiptID string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	d, ok := p.deliveries[receiptID]
	if !ok || d.Status != storage.StatusInflight {
		return false, nil
	}
	delete(p.deliveries, receiptID)
	return true, nil
}

func (p *Provider) RequeueStaleDeliveries(_ context.Context, before time.Time) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, d := range p.deliveries {
		if d.Status != storage.StatusInflight || !d.VisibleAt.Before(before) {
			continue
		}
		if d.Attempts >= d.MaxAttempts {
			d.Status = storage.StatusDead
		} else {
			d.Status = storage.StatusPending
			d.VisibleAt = time.Time{}
			count++
		}
	}
	return count, nil
}

func (p *Provider) SubscribeToInserts(callback func()) (func(), error) {
	p.cbMu.Lock()
	id := p.nextCbID
	p.nextCbID++
	p.insertCallbacks[id] = callback
	p.cbMu.Unlock()
	return func() {
		p.cbMu.Lock()
		delete(p.insertCallbacks, id)
		p.cbMu.Unlock()
	}, nil
}

func (p *Provider) emitInsert() {
	p.cbMu.Lock()
	cbs := make([]func(), 0, len(p.insertCallbacks))
	for _, cb := range p.insertCallbacks {
		cbs = append(cbs, cb)
	}
	p.cbMu.Unlock()
	for _, cb := range cbs {
		cb()
	}
}
