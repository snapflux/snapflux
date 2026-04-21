package broker

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"vinr.eu/snapflux/internal/config"
	"vinr.eu/snapflux/internal/storage"
)

type Broker struct {
	store        storage.Provider
	cfg          *config.Config
	id           string
	address      string
	ringMu       sync.RWMutex
	ring         *HashRing
	cancelTimers context.CancelFunc

	heartbeats     metric.Int64Counter
	evictions      metric.Int64Counter
	ringRecomputes metric.Int64Counter
}

func New(store storage.Provider, cfg *config.Config) *Broker {
	b := &Broker{store: store, cfg: cfg}
	meter := otel.GetMeterProvider().Meter("snapflux/broker")
	b.heartbeats, _ = meter.Int64Counter("snapflux.broker.heartbeats",
		metric.WithDescription("Total heartbeats sent"))
	b.evictions, _ = meter.Int64Counter("snapflux.broker.evictions",
		metric.WithDescription("Total stale brokers evicted"))
	b.ringRecomputes, _ = meter.Int64Counter("snapflux.broker.ring.recomputes",
		metric.WithDescription("Total hash ring recomputations"))
	_, _ = meter.Int64ObservableGauge("snapflux.broker.nodes",
		metric.WithDescription("Current number of active broker nodes in the ring"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(b.NodeCount()))
			return nil
		}),
	)
	return b
}

func (b *Broker) Start(ctx context.Context) error {
	b.id = rand.Text()

	host, _ := os.Hostname()
	b.address = b.cfg.BrokerAddress
	if b.address == "" {
		b.address = "http://" + host + ":" + b.cfg.Port
	}

	if err := b.store.InitBroker(ctx, b.id, host, b.address); err != nil {
		return err
	}

	brokers, err := b.store.GetActiveBrokers(ctx)
	if err != nil {
		return err
	}
	b.ringMu.Lock()
	b.ring = newHashRing(brokers)
	b.ringMu.Unlock()
	slog.Info("broker registered", "id", b.id, "address", b.address, "ringSize", b.NodeCount())

	tickerCtx, cancel := context.WithCancel(context.Background())
	b.cancelTimers = cancel
	heartbeatInterval := time.Duration(b.cfg.HeartbeatIntervalMs) * time.Millisecond

	go b.runHeartbeat(tickerCtx, heartbeatInterval)
	go b.runWatcher(tickerCtx, heartbeatInterval)

	return nil
}

func (b *Broker) Stop(ctx context.Context) {
	if b.cancelTimers != nil {
		b.cancelTimers()
	}
	if err := b.store.RemoveBroker(ctx, b.id); err != nil {
		slog.Error("failed to deregister broker", "error", err)
	}
}

func (b *Broker) IsSelf(key string) bool {
	b.ringMu.RLock()
	r := b.ring
	b.ringMu.RUnlock()
	if r == nil {
		return true
	}
	node, err := r.getNode(key)
	if err != nil {
		return true
	}
	return node.ID == b.id
}

func (b *Broker) GetNode(key string) (storage.BrokerEntity, error) {
	b.ringMu.RLock()
	r := b.ring
	b.ringMu.RUnlock()
	if r == nil {
		return storage.BrokerEntity{}, errors.New("ring not initialised")
	}
	return r.getNode(key)
}

func (b *Broker) NodeCount() int {
	b.ringMu.RLock()
	r := b.ring
	b.ringMu.RUnlock()
	if r == nil {
		return 0
	}
	return r.nodeCount()
}

func (b *Broker) runHeartbeat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.store.UpdateHeartbeat(context.Background(), b.id); err != nil {
				slog.Error("heartbeat failed", "error", err)
			} else {
				b.heartbeats.Add(context.Background(), 1)
			}
		}
	}
}

func (b *Broker) runWatcher(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.evictAndRecompute()
		}
	}
}

func (b *Broker) evictAndRecompute() {
	ctx := context.Background()
	timeout := time.Duration(b.cfg.HeartbeatTimeoutMs) * time.Millisecond
	before := time.Now().Add(-timeout)

	evicted, err := b.store.EvictStaleBrokers(ctx, before, b.id)
	if err != nil {
		slog.Error("ring watch failed", "error", err)
		return
	}

	brokers, err := b.store.GetActiveBrokers(ctx)
	if err != nil {
		slog.Error("ring watch failed", "error", err)
		return
	}

	b.ringMu.Lock()
	unchanged := len(evicted) == 0 && b.ring != nil && len(brokers) == b.ring.nodeCount()
	if !unchanged {
		b.ring = newHashRing(brokers)
	}
	b.ringMu.Unlock()
	if unchanged {
		return
	}
	b.ringRecomputes.Add(ctx, 1)
	if len(evicted) > 0 {
		b.evictions.Add(ctx, int64(len(evicted)))
		slog.Warn("evicted stale brokers — ring recomputed", "evicted", len(evicted), "nodes", len(brokers))
	} else {
		slog.Info("new brokers detected — ring recomputed", "nodes", len(brokers))
	}
}
