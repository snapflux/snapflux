# Configuration

All configuration is provided through environment variables. No configuration file is required.

---

## Server

| Variable | Default | Description |
|---|---|---|
| `PORT` | `5050` | HTTP port the server listens on. |
| `BROKER_ADDRESS` | `http://<hostname>:<PORT>` | Publicly reachable address of this broker, used for inter-broker forwarding. Set this explicitly in Kubernetes to the Pod IP or service DNS name. |

---

## Storage

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | _(empty)_ | MongoDB connection string (e.g. `mongodb://user:pass@host:27017/db`). If empty, the in-memory backend is used. |
| `MESSAGE_TTL_SECONDS` | `604800` | How long messages are retained (7 days). |

---

## Messaging

| Variable | Default | Description |
|---|---|---|
| `VISIBILITY_TIMEOUT_MS` | `30000` | How long a received message is hidden from other consumers before being requeued (30 seconds). |
| `MAX_DELIVERY_ATTEMPTS` | `5` | Maximum number of times a delivery is attempted before being abandoned. |
| `REQUEUE_INTERVAL_MS` | `10000` | How often the requeue job runs to reclaim stale in-flight deliveries (10 seconds). |

---

## BatchFlush

| Variable | Default | Description |
|---|---|---|
| `BATCH_FLUSH_INTERVAL_MS` | `50` | Maximum time between batch flushes in milliseconds. Lower values reduce latency for `fire-and-forget` and `durable` messages; higher values improve throughput. |
| `BATCH_FLUSH_SIZE` | `1000` | Number of queued messages that triggers an immediate flush, regardless of the interval. |

---

## Write-Ahead Log (WAL)

| Variable | Default | Description |
|---|---|---|
| `WAL_PATH` | `/tmp/snapflux/wal` | Directory where the WAL file and checkpoint are stored. Use a persistent volume in Kubernetes. |
| `WAL_MAX_BYTES` | `104857600` | Maximum WAL file size in bytes (100 MiB). |
| `WAL_HIGH_WATER_PCT` | `80` | WAL usage percentage at which new durable writes are rejected (backpressure). |
| `WAL_LOW_WATER_PCT` | `40` | WAL usage percentage at which writes resume after backpressure. |

---

## Broker

| Variable | Default | Description |
|---|---|---|
| `HEARTBEAT_INTERVAL_MS` | `5000` | How often this broker updates its heartbeat in storage (5 seconds). |
| `HEARTBEAT_TIMEOUT_MS` | `15000` | How long a broker can go without a heartbeat before being evicted from the ring (15 seconds). |

---

## Authentication

| Variable | Default | Description |
|---|---|---|
| `AUTH_KEYS` | _(empty)_ | Comma-separated list of `name:key` pairs for external client authentication. If empty, authentication is disabled. Example: `service-a:key1,service-b:key2`. |
| `BROKER_AUTH_KEY` | _(empty)_ | Bearer token used by this broker when forwarding requests to peer brokers. Validated independently of `AUTH_KEYS`. Required when auth is enabled in a multi-broker setup. |

See [Authentication](authentication.md) for setup guidance.

---

## Observability

Snapflux respects the standard OpenTelemetry environment variables for trace export:

| Variable | Description |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint (e.g. `http://otel-collector:4318`). If unset, trace export is disabled. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Optional headers for the OTLP exporter (e.g. `Authorization=Bearer token`). |
| `OTEL_SERVICE_NAME` | Service name reported in traces and metrics. Defaults to `unknown_service`. |

Prometheus metrics are always available at `GET /metrics` regardless of OTLP configuration.

See [Observability](observability.md) for the full metrics reference.

---

## Example: production Kubernetes Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: snapflux-secrets
type: Opaque
stringData:
  database-url: "mongodb://snapflux:password@mongo-svc:27017/snapflux"
  auth-keys: "billing-svc:aHR0cHM,inventory-svc:bG9yZW1p"
  broker-auth-key: "aW50ZXJuYWxicm9rZXJrZXk"
```
