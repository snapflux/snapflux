# Observability

Snapflux is fully instrumented with [OpenTelemetry](https://opentelemetry.io). Metrics are always available via a
Prometheus-compatible `/metrics` endpoint. Distributed traces are exported via OTLP when configured.

---

## Prometheus metrics

Metrics are available at `GET /metrics` in Prometheus text format. No authentication is required, so Prometheus can
scrape the endpoint without credentials.

### HTTP

These metrics are automatically produced by the `otelhttp` instrumentation for each API route.

| Metric                                 | Type      | Description                                                                                        |
|----------------------------------------|-----------|----------------------------------------------------------------------------------------------------|
| `http_server_request_duration_seconds` | Histogram | Request latency, labelled by `http.route`, `http.request.method`, and `http.response.status_code`. |

### Messaging

| Metric                              | Type    | Labels                                 | Description                                      |
|-------------------------------------|---------|----------------------------------------|--------------------------------------------------|
| `snapflux_messages_sent_total`      | Counter | `topic`, `durability`                  | Total messages successfully sent.                |
| `snapflux_messages_received_total`  | Counter | `topic`, `group`                       | Total message deliveries returned to consumers.  |
| `snapflux_messages_acked_total`     | Counter | `topic`, `group`                       | Total deliveries acknowledged.                   |
| `snapflux_messages_forwarded_total` | Counter | `operation` (`send`\|`receive`\|`ack`) | Total requests forwarded to another broker node. |

### Batch flush

| Metric                                  | Type      | Description                                            |
|-----------------------------------------|-----------|--------------------------------------------------------|
| `snapflux_batch_flushes_total`          | Counter   | Total number of batch flush operations.                |
| `snapflux_batch_flush_duration_seconds` | Histogram | Time taken per flush, in seconds.                      |
| `snapflux_batch_flush_size`             | Histogram | Number of messages written per flush.                  |
| `snapflux_batch_queue_depth`            | Gauge     | Current number of messages waiting in the flush queue. |

### Write-Ahead Log

| Metric                                   | Type    | Description                                                             |
|------------------------------------------|---------|-------------------------------------------------------------------------|
| `snapflux_wal_appends_total`             | Counter | Total WAL entries written.                                              |
| `snapflux_wal_acknowledges_total`        | Counter | Total WAL checkpoints advanced (batches flushed).                       |
| `snapflux_wal_backpressure_events_total` | Counter | Number of times the WAL entered backpressure (high-water mark reached). |
| `snapflux_wal_bytes`                     | Gauge   | Current WAL file size in bytes.                                         |

### Broker

| Metric                                  | Type    | Description                                                   |
|-----------------------------------------|---------|---------------------------------------------------------------|
| `snapflux_broker_heartbeats_total`      | Counter | Total successful heartbeats sent to storage.                  |
| `snapflux_broker_evictions_total`       | Counter | Total stale broker nodes evicted from the ring.               |
| `snapflux_broker_ring_recomputes_total` | Counter | Total hash ring recomputations triggered by topology changes. |
| `snapflux_broker_nodes`                 | Gauge   | Current number of active broker nodes in the ring.            |

---

## Prometheus scrape config

```yaml
scrape_configs:
  - job_name: snapflux
    static_configs:
      - targets: [ "snapflux:5050" ]
    metrics_path: /metrics
```

In Kubernetes, use a `ServiceMonitor` (Prometheus Operator):

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: snapflux
spec:
  selector:
    matchLabels:
      app: snapflux
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
```

---

## Useful Prometheus queries

**Send throughput by durability mode:**

```promql
sum(rate(snapflux_messages_sent_total[1m])) by (durability)
```

**Receive throughput by topic:**

```promql
sum(rate(snapflux_messages_received_total[1m])) by (topic)
```

**Pending flush queue depth across all nodes:**

```promql
sum(snapflux_batch_queue_depth)
```

**WAL usage as a percentage of max:**

```promql
snapflux_wal_bytes / <WAL_MAX_BYTES> * 100
```

**Backpressure events per minute:**

```promql
rate(snapflux_wal_backpressure_events_total[1m])
```

**P99 flush latency:**

```promql
histogram_quantile(0.99, sum(rate(snapflux_batch_flush_duration_seconds_bucket[5m])) by (le))
```

**P99 HTTP request latency per route:**

```promql
histogram_quantile(0.99,
  sum(rate(http_server_request_duration_seconds_bucket[5m])) by (le, http_route)
)
```

**Broker node count (should equal replica count):**

```promql
snapflux_broker_nodes
```

---

## Distributed tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable trace export:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
OTEL_SERVICE_NAME=snapflux
```

Traces are exported via OTLP/HTTP. When the variable is not set, the trace provider runs in noop mode — no overhead.

### Instrumented operations

| Span                                             | Created by        | Key attributes                            |
|--------------------------------------------------|-------------------|-------------------------------------------|
| `POST /v1/topics/{topic}/messages`               | HTTP middleware   | `http.route`, `http.response.status_code` |
| `GET /v1/topics/{topic}/messages`                | HTTP middleware   | `http.route`, `http.response.status_code` |
| `DELETE /v1/topics/{topic}/messages/{receiptId}` | HTTP middleware   | `http.route`, `http.response.status_code` |
| `snapflux.send`                                  | Messaging service | `topic`, `durability`                     |
| `snapflux.receive`                               | Messaging service | `topic`, `group`                          |
| `snapflux.ack`                                   | Messaging service | `topic`, `group`                          |

Errors are recorded on the relevant span with `span.RecordError` and the span status is set to `Error`.

### W3C trace context propagation

Snapflux propagates the `traceparent` and `tracestate` headers. If a client sends a request with a valid `traceparent`
header, Snapflux will create child spans under the caller's trace, giving end-to-end visibility from client through
broker into storage.

### Example: OpenTelemetry Collector config

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [ otlp ]
      exporters: [ jaeger ]
```
