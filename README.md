# Snapflux

**Kubernetes-native message queue and event store with pluggable storage backends.**

Snapflux is a lightweight, self-hosted message broker designed to run inside a Kubernetes cluster. It exposes a simple HTTP API, distributes messages across nodes using a consistent hash ring, and persists events to your own database — no proprietary cloud service required.

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8.svg)](https://golang.org)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-enabled-f5a800.svg)](https://opentelemetry.io)

---

## Features

- **HTTP API** — send, receive, and acknowledge messages over plain HTTP
- **Pluggable storage** — MongoDB for production, in-memory for development and testing
- **WAL durability** — write-ahead log with configurable high/low water backpressure
- **Broker distribution** — consistent hash ring routes messages to the correct node in a multi-broker cluster
- **Three durability modes** — `fire-and-forget`, `durable` (WAL + async flush), and `strict` (synchronous write)
- **Requeue** — stale in-flight deliveries are automatically reclaimed after a visibility timeout
- **Observability** — OpenTelemetry traces (OTLP) and Prometheus metrics at `/metrics`
- **Authentication** — Bearer token API keys with a separate credential for inter-broker communication

---

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | Components, durability modes, broker distribution, WAL design |
| [API Reference](docs/api.md) | HTTP endpoints, request/response schemas, examples |
| [Configuration](docs/configuration.md) | All environment variables with defaults |
| [Authentication](docs/authentication.md) | API keys, broker credentials, Kubernetes Secrets |
| [Observability](docs/observability.md) | Prometheus metrics reference, distributed tracing |
| [Contributing](CONTRIBUTING.md) | Development setup, submitting changes |
| [Security](SECURITY.md) | Reporting vulnerabilities, security model |

---

## Quick Start

### Single node (in-memory storage)

```bash
docker run --rm -p 5050:5050 ghcr.io/snapflux/snapflux:latest
```

### Send a message

```bash
curl -X POST http://localhost:5050/v1/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "order-123", "body": {"amount": 99.00}, "durability": "durable"}'
```

### Receive messages

```bash
curl "http://localhost:5050/v1/topics/orders/messages?group=billing&limit=10"
```

### Acknowledge a message

```bash
curl -X DELETE \
  "http://localhost:5050/v1/topics/orders/messages/{receiptId}?group=billing"
```

### With MongoDB persistence

```bash
docker run --rm -p 5050:5050 \
  -e DATABASE_URL=mongodb://mongo:27017 \
  ghcr.io/snapflux/snapflux:latest
```

---

## Multi-Broker Deployment (Kubernetes)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: snapflux
spec:
  replicas: 3
  selector:
    matchLabels:
      app: snapflux
  template:
    metadata:
      labels:
        app: snapflux
    spec:
      containers:
        - name: snapflux
          image: ghcr.io/snapflux/snapflux:latest
          ports:
            - containerPort: 5050
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: snapflux-secrets
                  key: database-url
            - name: AUTH_KEYS
              valueFrom:
                secretKeyRef:
                  name: snapflux-secrets
                  key: auth-keys
            - name: BROKER_AUTH_KEY
              valueFrom:
                secretKeyRef:
                  name: snapflux-secrets
                  key: broker-auth-key
            - name: BROKER_ADDRESS
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
```

---

## Client Libraries

| Library | Language | Package |
|---|---|---|
| [NestJS client](clients/nestjs) | TypeScript / NestJS | `@snapflux/nestjs` |

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

---

## License

Snapflux is licensed under the [GNU Affero General Public License v3.0](LICENSE).
