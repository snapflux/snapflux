package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vinr.eu/snapflux/internal/broker"
	"vinr.eu/snapflux/internal/config"
	"vinr.eu/snapflux/internal/messaging"
	"vinr.eu/snapflux/internal/server"
	storagepkg "vinr.eu/snapflux/internal/storage"
	"vinr.eu/snapflux/internal/storage/memory"
	"vinr.eu/snapflux/internal/storage/mongodb"
	"vinr.eu/snapflux/internal/telemetry"
	"vinr.eu/snapflux/internal/wal"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := config.Load()

	ctx := context.Background()

	telShutdown, metricsHandler, err := telemetry.Setup(ctx)
	if err != nil {
		slog.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}

	var store storagepkg.Provider
	if cfg.DatabaseURL != "" {
		slog.Info("storage backend selected", "type", "mongodb")
		store = mongodb.New()
	} else {
		slog.Info("storage backend selected", "type", "memory")
		store = memory.New()
	}

	if err := store.Connect(ctx, cfg.DatabaseURL); err != nil {
		slog.Error("failed to connect to storage", "error", err)
		os.Exit(1)
	}

	walSvc, pendingEntries, err := wal.New(cfg)
	if err != nil {
		slog.Error("failed to initialize WAL", "error", err)
		os.Exit(1)
	}

	batchFlush := messaging.NewBatchFlush(store, walSvc, cfg)
	if err := batchFlush.Start(ctx, pendingEntries); err != nil {
		slog.Error("failed to start batch flush", "error", err)
		os.Exit(1)
	}

	brokerSvc := broker.New(store, cfg)
	if err := brokerSvc.Start(ctx); err != nil {
		slog.Error("failed to start broker", "error", err)
		os.Exit(1)
	}

	requeueSvc := messaging.NewRequeue(store, cfg)
	if err := requeueSvc.Start(ctx); err != nil {
		slog.Error("failed to start requeue", "error", err)
		os.Exit(1)
	}

	msgSvc := messaging.NewService(store, brokerSvc, batchFlush, walSvc, cfg)
	srv := server.New(msgSvc, store, brokerSvc, walSvc, cfg, metricsHandler)

	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("server error", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_ = srv.Stop(shutdownCtx)
	requeueSvc.Stop()
	brokerSvc.Stop(shutdownCtx)
	batchFlush.Stop(shutdownCtx)
	_ = walSvc.Close()
	_ = store.Close(shutdownCtx)
	_ = telShutdown(shutdownCtx)

	slog.Info("shutdown complete")
}
