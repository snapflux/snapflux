package messaging

import (
	"context"
	"log/slog"
	"time"

	"snapflux/api-service-go/internal/config"
	"snapflux/api-service-go/internal/storage"
)

type Requeue struct {
	store       storage.Provider
	cfg         *config.Config
	cancelTimer context.CancelFunc
	unsubscribe func()
}

func NewRequeue(store storage.Provider, cfg *config.Config) *Requeue {
	return &Requeue{store: store, cfg: cfg}
}

func (r *Requeue) Start(_ context.Context) error {
	unsub, err := r.store.SubscribeToInserts(func() {
		r.requeue()
	})
	if err != nil {
		return err
	}
	r.unsubscribe = unsub

	if r.cfg.RequeueIntervalMs <= 0 {
		return nil
	}

	timerCtx, cancel := context.WithCancel(context.Background())
	r.cancelTimer = cancel
	interval := time.Duration(r.cfg.RequeueIntervalMs) * time.Millisecond

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-timerCtx.Done():
				return
			case <-ticker.C:
				r.requeue()
			}
		}
	}()

	return nil
}

func (r *Requeue) Stop() {
	if r.cancelTimer != nil {
		r.cancelTimer()
	}
	if r.unsubscribe != nil {
		r.unsubscribe()
	}
}

func (r *Requeue) requeue() {
	count, err := r.store.RequeueStaleDeliveries(context.Background(), time.Now())
	if err != nil {
		slog.Error("requeue failed", "error", err)
		return
	}
	if count > 0 {
		slog.Info("re-queued stale deliveries", "count", count)
	}
}
