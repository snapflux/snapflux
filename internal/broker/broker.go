package broker

import (
	"context"
	"log/slog"
	"os"
	"time"

	"crypto/rand"

	"snapflux/api-service-go/internal/config"
	"snapflux/api-service-go/internal/storage"
)

type Broker struct {
	store        storage.Provider
	cfg          *config.Config
	id           string
	address      string
	ring         *HashRing
	cancelTimers context.CancelFunc
}

func New(store storage.Provider, cfg *config.Config) *Broker {
	return &Broker{store: store, cfg: cfg}
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
	b.ring = newHashRing(brokers)
	slog.Info("broker registered", "id", b.id, "address", b.address, "ringSize", b.ring.nodeCount())

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
	node, err := b.ring.getNode(key)
	if err != nil {
		return true
	}
	return node.ID == b.id
}

func (b *Broker) GetNode(key string) (storage.BrokerEntity, error) {
	return b.ring.getNode(key)
}

func (b *Broker) NodeCount() int {
	if b.ring == nil {
		return 0
	}
	return b.ring.nodeCount()
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

	if len(evicted) == 0 && len(brokers) == b.ring.nodeCount() {
		return
	}

	b.ring = newHashRing(brokers)
	if len(evicted) > 0 {
		slog.Warn("evicted stale brokers — ring recomputed", "evicted", len(evicted), "nodes", len(brokers))
	} else {
		slog.Info("new brokers detected — ring recomputed", "nodes", len(brokers))
	}
}
