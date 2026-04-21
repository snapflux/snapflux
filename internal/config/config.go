package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port          string
	BrokerAddress string
	DatabaseURL   string
	// AuthKeys maps client name → API key. Empty means auth is disabled.
	// Env: AUTH_KEYS=name:key,name:key,...
	AuthKeys map[string]string
	// BrokerAuthKey is the Bearer token this broker sends when forwarding
	// requests to peer nodes, and is validated independently of AuthKeys.
	// Env: BROKER_AUTH_KEY=key
	BrokerAuthKey        string
	MessageTTLSeconds    int
	VisibilityTimeoutMs  int
	MaxDeliveryAttempts  int
	HeartbeatIntervalMs  int
	HeartbeatTimeoutMs   int
	BatchFlushIntervalMs int
	BatchFlushSize       int
	BatchMaxQueueDepth   int
	RequeueIntervalMs    int
	WALPath              string
	WALMaxBytes          int64
	WALHighWaterPct      int
	WALLowWaterPct       int
}

func Load() *Config {
	return &Config{
		Port:                 env("PORT", "5050"),
		BrokerAddress:        env("BROKER_ADDRESS", ""),
		DatabaseURL:          env("DATABASE_URL", ""),
		MessageTTLSeconds:    envInt("MESSAGE_TTL_SECONDS", 604800),
		VisibilityTimeoutMs:  envInt("VISIBILITY_TIMEOUT_MS", 30000),
		MaxDeliveryAttempts:  envInt("MAX_DELIVERY_ATTEMPTS", 5),
		HeartbeatIntervalMs:  envInt("HEARTBEAT_INTERVAL_MS", 5000),
		HeartbeatTimeoutMs:   envInt("HEARTBEAT_TIMEOUT_MS", 15000),
		BatchFlushIntervalMs: envInt("BATCH_FLUSH_INTERVAL_MS", 50),
		BatchFlushSize:       envInt("BATCH_FLUSH_SIZE", 1000),
		BatchMaxQueueDepth:   envInt("BATCH_MAX_QUEUE_DEPTH", 50000),
		RequeueIntervalMs:    envInt("REQUEUE_INTERVAL_MS", 10000),
		WALPath:              env("WAL_PATH", "/tmp/snapflux/wal"),
		WALMaxBytes:          int64(envInt("WAL_MAX_BYTES", 104857600)),
		WALHighWaterPct:      envInt("WAL_HIGH_WATER_PCT", 80),
		WALLowWaterPct:       envInt("WAL_LOW_WATER_PCT", 40),
		AuthKeys:             envAuthKeys("AUTH_KEYS"),
		BrokerAuthKey:        env("BROKER_AUTH_KEY", ""),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envAuthKeys parses "name:key,name:key,..." into a map[name]key.
func envAuthKeys(key string) map[string]string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	m := make(map[string]string)
	for _, pair := range strings.Split(v, ",") {
		pair = strings.TrimSpace(pair)
		name, token, ok := strings.Cut(pair, ":")
		if ok && name != "" && token != "" {
			m[strings.TrimSpace(name)] = strings.TrimSpace(token)
		}
	}
	return m
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
