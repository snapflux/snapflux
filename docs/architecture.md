# Architecture

## Overview

Snapflux is a horizontally scalable message queue built around a simple principle: messages are stored in **your own
database**, and the broker layer is stateless enough to run as a standard Kubernetes `Deployment`. Multiple broker nodes
coordinate through shared storage and a consistent hash ring — no separate coordination plane (like ZooKeeper or etcd)
is required.

---

## Components

```
┌─────────────────────────────────────────────────────────┐
│                        Client                           │
└───────────────────────────┬─────────────────────────────┘
                            │ HTTP
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    HTTP Server                          │
│   POST /v1/topics/{topic}/messages  (send)              │
│   GET  /v1/topics/{topic}/messages  (receive)           │
│   DELETE /v1/topics/{topic}/messages/{id} (ack)         │
│   GET /health   GET /metrics                            │
└───────────┬─────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────┐     ┌───────────────────────────┐
│   Messaging Service   │────▶│      Broker / Hash Ring   │
│  Send / Receive / Ack │     │  Self-registration        │
└──────────┬────────────┘     │  Heartbeat + eviction     │
           │                  │  Forward to correct node  │
           ▼                  └───────────────────────────┘
┌──────────────────────┐
│      BatchFlush      │◀─── WAL (Write-Ahead Log)
│  Buffers DB writes   │     Backpressure control
│  Replays on startup  │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Storage Provider   │
│  MongoDB | Memory    │
└──────────────────────┘
```

---

## Startup Order

The following sequence ensures each component has its dependencies ready before starting:

1. **Config** — load all settings from environment variables
2. **Telemetry** — initialise OpenTelemetry metric and trace providers
3. **Storage** — connect to MongoDB (or initialise in-memory store)
4. **WAL** — open the write-ahead log file; replay any uncommitted entries from a previous run
5. **BatchFlush** — start the periodic flush timer; replay WAL entries recovered in step 4
6. **Broker** — register this node; build the initial hash ring from active brokers in storage
7. **Requeue** — start the background job that reclaims stale in-flight deliveries
8. **MessagingService** — wire together Broker, BatchFlush, WAL, and Storage
9. **HTTPServer** — begin accepting connections

Shutdown reverses this order with a 30-second timeout.

---

## Message Routing

Each message carries a **key**. The hash ring maps that key to a specific broker node:

- If the key maps to **this node**, the message is handled locally.
- If the key maps to **another node**, the request is forwarded with an `X-Snapflux-Hop: 1` header so the target node
  handles it directly without re-routing.

Receive and acknowledge operations are similarly routed using `topic:group` as the routing key, ensuring all deliveries
for a consumer group are handled by the same node.

The hash ring uses SHA-256 with 150 virtual nodes per broker for even distribution. It is recomputed whenever a
heartbeat watch detects topology changes (new brokers or evicted stale nodes).

---

## Durability Modes

Each message is sent with one of three durability guarantees:

### `fire-and-forget`

The message is placed in the in-memory `BatchFlush` queue immediately and acknowledged to the caller. It will be written
to storage on the next flush cycle (default every 50 ms). If the process crashes before the flush, the message is lost.

**Use when:** maximum throughput matters and occasional loss is acceptable (metrics, logs).

### `durable` (default)

The message is written to the **write-ahead log** (WAL) before being acknowledged to the caller. The WAL entry is
flushed to disk synchronously (`fsync`). BatchFlush later writes it to storage and advances the WAL checkpoint. If the
process crashes before the storage write, the entry is replayed from the WAL on next startup.

**Use when:** you need delivery guarantees without the latency of a synchronous storage write.

### `strict`

The message is written directly to storage (MongoDB) before the call returns. No WAL is used. This is the slowest mode
but guarantees the message is in the database before the client receives a success response.

**Use when:** the database is the authoritative record and the caller must not proceed until the write is confirmed.

---

## Write-Ahead Log (WAL)

The WAL is a single append-only file (`wal.log`) stored at `WAL_PATH`. Each entry is a JSON-encoded line. A separate
`checkpoint` file tracks the highest sequence number that has been successfully flushed to storage.

**Backpressure:** when the WAL file size reaches `WAL_HIGH_WATER_PCT` (default 80%) of `WAL_MAX_BYTES`, new durable
writes are rejected with `503 Service Unavailable`. Writes resume once the file compacts below `WAL_LOW_WATER_PCT` (
default 40%). This prevents unbounded memory/disk growth under sustained write pressure.

**Compaction:** after each `Acknowledge` call, if the file exceeds the low-water mark, entries below the checkpoint are
removed by rewriting the file.

---

## BatchFlush

BatchFlush batches `BulkSendMessages` and `BulkCreateDeliveries` calls to reduce storage round-trips. It flushes on two
triggers:

- **Timer** — every `BATCH_FLUSH_INTERVAL_MS` milliseconds (default 50 ms)
- **Size** — when the queue reaches `BATCH_FLUSH_SIZE` entries (default 1000)

Only one flush runs at a time. If a flush fails, all entries are prepended back to the queue and retried on the next
cycle.

---

## Subscriptions and Deliveries

Snapflux uses a **consumer group** model. When a consumer first calls receive for a topic+group pair, a subscription is
created. Subsequent sends to that topic create a delivery record for each active group, giving every group its own copy
of the message.

A delivery moves through the following states:

```
pending → in-flight (received) → acknowledged
                ↓
          visibility timeout → pending (requeue)
```

If a delivery is not acknowledged within `VISIBILITY_TIMEOUT_MS`, the Requeue job reclaims it and makes it visible
again. After `MAX_DELIVERY_ATTEMPTS` failed attempts, the delivery is abandoned.

---

## Storage Backends

| Backend   | When to use           | Notes                                     |
|-----------|-----------------------|-------------------------------------------|
| MongoDB   | Production            | Full persistence; requires `DATABASE_URL` |
| In-memory | Development / testing | No persistence; data lost on restart      |

The `storage.Provider` interface makes backends interchangeable. Both backends implement the same contract for messages,
deliveries, subscriptions, and broker registration.
