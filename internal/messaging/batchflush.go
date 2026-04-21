package messaging

import (
	"context"
	"crypto/rand"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"vinr.eu/snapflux/internal/config"
	"vinr.eu/snapflux/internal/storage"
	"vinr.eu/snapflux/internal/wal"
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

	flushCount    metric.Int64Counter
	flushDuration metric.Float64Histogram
	flushSize     metric.Int64Histogram
}

func NewBatchFlush(store storage.Provider, walSvc *wal.WAL, cfg *config.Config) *BatchFlush {
	b := &BatchFlush{store: store, walSvc: walSvc, cfg: cfg}
	meter := otel.GetMeterProvider().Meter("snapflux/batch")
	b.flushCount, _ = meter.Int64Counter("snapflux.batch.flushes",
		metric.WithDescription("Total batch flush operations"))
	b.flushDuration, _ = meter.Float64Histogram("snapflux.batch.flush.duration",
		metric.WithDescription("Batch flush duration"),
		metric.WithUnit("s"))
	b.flushSize, _ = meter.Int64Histogram("snapflux.batch.flush.size",
		metric.WithDescription("Messages per batch flush"))
	_, _ = meter.Int64ObservableGauge("snapflux.batch.queue.depth",
		metric.WithDescription("Current number of messages waiting to be flushed"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			b.mu.Lock()
			depth := int64(len(b.queue))
			b.mu.Unlock()
			o.Observe(depth)
			return nil
		}),
	)
	return b
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
	// Fire-and-forget messages (no WAL backing) are dropped when the queue is full.
	// Durable messages are always accepted because the WAL ensures recovery on restart.
	if walEntry == nil && b.cfg.BatchMaxQueueDepth > 0 && len(b.queue) >= b.cfg.BatchMaxQueueDepth {
		b.mu.Unlock()
		slog.Warn("batch queue full — dropping fire-and-forget message", "topic", topic, "id", id)
		return
	}
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
	start := time.Now()
	err := b.flushEntries(ctx, batch)
	b.flushCount.Add(ctx, 1)
	b.flushDuration.Record(ctx, time.Since(start).Seconds())
	b.flushSize.Record(ctx, int64(len(batch)))
	if err != nil {
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
