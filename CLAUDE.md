# Snapflux

Kubernetes-native message queue / event store. HTTP API with pluggable storage backends, WAL durability, and
broker-based distribution.

## Stack

- **Go 1.26**, MongoDB driver
- Entry point: `cmd/api/main.go`
- Config: environment variables via `config.Load()` — no config file needed

## Key Packages

| Package              | Role                                                                        |
|----------------------|-----------------------------------------------------------------------------|
| `internal/server`    | HTTP server — send, receive, ack, health endpoints                          |
| `internal/messaging` | Core send/receive/ack logic; `BatchFlush` buffers DB writes                 |
| `internal/broker`    | Self-registration, heartbeats, hash ring for message distribution           |
| `internal/wal`       | Write-Ahead Log with high/low water backpressure                            |
| `internal/storage`   | `Provider` interface; backends: `mongodb/` and `memory/`                    |
| `internal/config`    | Env-based config: ports, TTLs, batch sizes, WAL tuning                      |
| `internal/model`     | Request/response types; durability modes (fire-and-forget, durable, strict) |

## Storage Backends

- **MongoDB** — full persistence (set `DATABASE_URL`)
- **In-Memory** — dev/testing, no persistence (default when no `DATABASE_URL`)

## Startup Order

config → storage → WAL (recover pending) → BatchFlush → Broker → Requeue → MessagingService → HTTPServer

## Notable Patterns

- Pluggable storage via interface — swap MongoDB ↔ Memory transparently
- WAL backpressure rejects messages when high watermark is exceeded
- Requeue job periodically reclaims stale in-flight deliveries
- Graceful shutdown with 30s timeout, ordered stop sequence
