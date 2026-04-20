package messaging

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"crypto/rand"

	"snapflux/api-service-go/internal/config"
	"snapflux/api-service-go/internal/storage"
	"snapflux/api-service-go/internal/wal"
)

type flushEntry struct {
	walSeq    int // -1 if no WAL entry
	id        string
	topic     string
	key       string
	payload   any
	timestamp string
}

type BatchFlush struct {
	store       storage.Provider
	walSvc      *wal.WAL
	cfg         *config.Config
	mu          sync.Mutex
	queue       []flushEntry
	flushing    bool
	cancelTimer context.CancelFunc
}

func NewBatchFlush(store storage.Provider, walSvc *wal.WAL, cfg *config.Config) *BatchFlush {
	return &BatchFlush{store: store, walSvc: walSvc, cfg: cfg}
}

func (b *BatchFlush) Start(ctx context.Context, pendingEntries []wal.Entry) error {
	if len(pendingEntries) > 0 {
		slog.Info("replaying WAL entries", "count", len(pendingEntries))
		entries := make([]flushEntry, len(pendingEntries))
		for i, e := range pendingEntries {
			entries[i] = flushEntry{walSeq: e.Seq, id: e.ID, topic: e.Topic, key: e.Key, payload: e.Payload, timestamp: e.Timestamp}
		}
		if err := b.flushEntries(ctx, entries); err != nil {
			return err
		}
	}

	if b.cfg.BatchFlushIntervalMs <= 0 {
		return nil
	}

	timerCtx, cancel := context.WithCancel(context.Background())
	b.cancelTimer = cancel
	interval := time.Duration(b.cfg.BatchFlushIntervalMs) * time.Millisecond

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-timerCtx.Done():
				return
			case <-ticker.C:
				b.flush(context.Background())
			}
		}
	}()

	return nil
}

func (b *BatchFlush) Stop(ctx context.Context) {
	if b.cancelTimer != nil {
		b.cancelTimer()
	}
	b.flush(ctx)
}

func (b *BatchFlush) Enqueue(walEntry *wal.Entry, id, topic, key string, payload any, timestamp string) {
	entry := flushEntry{walSeq: -1, id: id, topic: topic, key: key, payload: payload, timestamp: timestamp}
	if walEntry != nil {
		entry.walSeq = walEntry.Seq
	}
	b.mu.Lock()
	b.queue = append(b.queue, entry)
	shouldFlush := len(b.queue) >= b.cfg.BatchFlushSize
	b.mu.Unlock()

	if shouldFlush {
		go b.flush(context.Background())
	}
}

func (b *BatchFlush) flush(ctx context.Context) {
	b.mu.Lock()
	if b.flushing || len(b.queue) == 0 {
		b.mu.Unlock()
		return
	}
	b.flushing = true
	size := b.cfg.BatchFlushSize
	if size > len(b.queue) {
		size = len(b.queue)
	}
	batch := make([]flushEntry, size)
	copy(batch, b.queue[:size])
	b.queue = b.queue[size:]
	b.mu.Unlock()

	slog.Debug("flushing batch", "count", len(batch))
	if err := b.flushEntries(ctx, batch); err != nil {
		slog.Error("batch flush failed — re-queuing entries", "error", err)
		b.mu.Lock()
		b.queue = append(batch, b.queue...)
		b.mu.Unlock()
	}

	b.mu.Lock()
	b.flushing = false
	b.mu.Unlock()
}

func (b *BatchFlush) flushEntries(ctx context.Context, entries []flushEntry) error {
	messages := make([]storage.MessageEntity, len(entries))
	for i, e := range entries {
		createdAt := time.Now()
		if e.timestamp != "" {
			if t, err := time.Parse(time.RFC3339Nano, e.timestamp); err == nil {
				createdAt = t
			}
		}
		messages[i] = storage.MessageEntity{ID: e.id, Topic: e.topic, Key: e.key, Payload: e.payload, CreatedAt: createdAt}
	}
	if err := b.store.BulkSendMessages(ctx, messages); err != nil {
		return err
	}

	byTopic := make(map[string][]flushEntry)
	for _, e := range entries {
		byTopic[e.topic] = append(byTopic[e.topic], e)
	}

	for topic, topicEntries := range byTopic {
		groups, err := b.store.GetSubscriptions(ctx, topic)
		if err != nil {
			return err
		}
		if len(groups) == 0 {
			continue
		}

		deliveries := make([]storage.DeliveryEntity, 0, len(topicEntries)*len(groups))
		now := time.Now()
		for _, e := range topicEntries {
			for _, group := range groups {
				deliveries = append(deliveries, storage.DeliveryEntity{
					ID:          rand.Text(),
					MessageID:   e.id,
					Topic:       e.topic,
					Group:       group,
					Status:      storage.StatusPending,
					VisibleAt:   now,
					Attempts:    0,
					MaxAttempts: b.cfg.MaxDeliveryAttempts,
					CreatedAt:   now,
				})
			}
		}
		if err := b.store.BulkCreateDeliveries(ctx, deliveries); err != nil {
			return err
		}
	}

	maxSeq := -1
	for _, e := range entries {
		if e.walSeq > maxSeq {
			maxSeq = e.walSeq
		}
	}
	if maxSeq >= 0 {
		return b.walSvc.Acknowledge(maxSeq)
	}
	return nil
}
