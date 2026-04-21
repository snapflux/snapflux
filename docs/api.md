# API Reference

Snapflux exposes a versioned HTTP API. All request and response bodies are JSON. All API routes require authentication
when `AUTH_KEYS` is configured (see [Authentication](authentication.md)).

Base path: `/v1`

---

## Endpoints

### Send a message

```
POST /v1/topics/{topic}/messages
```

**Path parameters**

| Parameter | Description                                  |
|-----------|----------------------------------------------|
| `topic`   | Topic name. Created implicitly on first use. |

**Request body**

| Field        | Type   | Required | Description                                                                                                |
|--------------|--------|----------|------------------------------------------------------------------------------------------------------------|
| `key`        | string | yes      | Routing key. Messages with the same key are always routed to the same broker node.                         |
| `body`       | any    | no       | Arbitrary JSON payload.                                                                                    |
| `durability` | string | no       | `fire-and-forget`, `durable` (default), or `strict`. See [Architecture](architecture.md#durability-modes). |

**Response**

| Status                    | Body                                             | When                                       |
|---------------------------|--------------------------------------------------|--------------------------------------------|
| `200 OK`                  | `{"id": "...", "durability": "durable"}`         | `durable` or `strict`                      |
| `202 Accepted`            | `{"id": "...", "durability": "fire-and-forget"}` | `fire-and-forget`                          |
| `400 Bad Request`         | `{"error": "..."}`                               | Missing or invalid fields                  |
| `503 Service Unavailable` | `{"error": "..."}`                               | WAL backpressure or broker routing failure |

**Example**

```bash
curl -X POST http://localhost:5050/v1/topics/orders/messages \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <api-key>" \
  -d '{
    "key": "order-123",
    "body": {"amount": 99.00, "currency": "EUR"},
    "durability": "durable"
  }'
```

```json
{
  "id": "ABC123XYZ",
  "durability": "durable"
}
```

---

### Receive messages

```
GET /v1/topics/{topic}/messages
```

Returns up to `limit` messages for the given consumer group. Each returned message is marked in-flight with a visibility
timeout. The caller must acknowledge each message before the timeout expires, or it will be requeued.

Calling receive for the first time on a topic+group pair creates the subscription. Only messages sent **after** the
subscription is created are delivered to that group.

**Path parameters**

| Parameter | Description |
|-----------|-------------|
| `topic`   | Topic name. |

**Query parameters**

| Parameter | Type    | Required | Default | Description                                    |
|-----------|---------|----------|---------|------------------------------------------------|
| `group`   | string  | yes      | —       | Consumer group name.                           |
| `limit`   | integer | no       | `10`    | Number of messages to return. Capped at `100`. |

**Response**

| Status                    | Body                                    | When                      |
|---------------------------|-----------------------------------------|---------------------------|
| `200 OK`                  | Array of message objects (may be empty) | Always on success         |
| `400 Bad Request`         | `{"error": "..."}`                      | Missing `group` parameter |
| `503 Service Unavailable` | `{"error": "..."}`                      | Broker routing failure    |

**Message object**

| Field       | Type    | Description                                                 |
|-------------|---------|-------------------------------------------------------------|
| `id`        | string  | Message ID.                                                 |
| `receiptId` | string  | Delivery receipt. Required to acknowledge this delivery.    |
| `key`       | string  | Routing key the message was sent with.                      |
| `body`      | any     | Original message payload.                                   |
| `attempts`  | integer | Number of times this delivery has been attempted.           |
| `sentAt`    | string  | ISO 8601 timestamp of when the message was originally sent. |

**Example**

```bash
curl "http://localhost:5050/v1/topics/orders/messages?group=billing&limit=5" \
  -H "Authorization: Bearer <api-key>"
```

```json
[
  {
    "id": "ABC123XYZ",
    "receiptId": "REC456DEF",
    "key": "order-123",
    "body": {
      "amount": 99.00,
      "currency": "EUR"
    },
    "attempts": 1,
    "sentAt": "2024-01-15T10:30:00.000000000Z"
  }
]
```

---

### Acknowledge a message

```
DELETE /v1/topics/{topic}/messages/{receiptId}
```

Marks a delivery as successfully processed. Once acknowledged, the message will not be redelivered to this consumer
group.

**Path parameters**

| Parameter   | Description                                       |
|-------------|---------------------------------------------------|
| `topic`     | Topic name.                                       |
| `receiptId` | The `receiptId` returned by the receive endpoint. |

**Query parameters**

| Parameter | Type   | Required | Description          |
|-----------|--------|----------|----------------------|
| `group`   | string | yes      | Consumer group name. |

**Response**

| Status                    | Body                                         | When                                      |
|---------------------------|----------------------------------------------|-------------------------------------------|
| `200 OK`                  | `{"receiptId": "...", "acknowledged": true}` | Successfully acknowledged                 |
| `400 Bad Request`         | `{"error": "..."}`                           | Missing `group` parameter                 |
| `404 Not Found`           | `{"error": "..."}`                           | Receipt not found or already acknowledged |
| `503 Service Unavailable` | `{"error": "..."}`                           | Broker routing failure                    |

**Example**

```bash
curl -X DELETE \
  "http://localhost:5050/v1/topics/orders/messages/REC456DEF?group=billing" \
  -H "Authorization: Bearer <api-key>"
```

```json
{
  "receiptId": "REC456DEF",
  "acknowledged": true
}
```

---

### Health check

```
GET /health
```

Returns the health of the broker. Does not require authentication. Suitable for Kubernetes liveness and readiness
probes.

**Response**

| Status                    | Body          | When                      |
|---------------------------|---------------|---------------------------|
| `200 OK`                  | Health object | All checks pass           |
| `503 Service Unavailable` | Health object | One or more checks failed |

**Health object**

| Field    | Type   | Description         |
|----------|--------|---------------------|
| `status` | string | `ok` or `error`     |
| `checks` | object | Named check results |

**Checks**

| Check     | Values                       | Description                     |
|-----------|------------------------------|---------------------------------|
| `storage` | `up` / `down: <reason>`      | Storage connectivity            |
| `wal`     | `accepting` / `backpressure` | WAL write availability          |
| `ring`    | `N node(s)` / `empty`        | Active brokers in the hash ring |

**Example**

```bash
curl http://localhost:5050/health
```

```json
{
  "status": "ok",
  "checks": {
    "storage": "up",
    "wal": "accepting",
    "ring": "3 node(s)"
  }
}
```

---

### Prometheus metrics

```
GET /metrics
```

Returns all metrics in Prometheus text format. Does not require authentication. See [Observability](observability.md)for
the full metrics reference.

---

## Error responses

All error responses use the following shape:

```json
{
  "error": "<human-readable message>"
}
```

| Status                      | Meaning                                                          |
|-----------------------------|------------------------------------------------------------------|
| `400 Bad Request`           | Invalid request (missing required field, malformed body)         |
| `401 Unauthorized`          | Missing or invalid `Authorization` header                        |
| `404 Not Found`             | Receipt ID not found or already acknowledged                     |
| `500 Internal Server Error` | Unexpected server error                                          |
| `503 Service Unavailable`   | WAL backpressure, storage unavailable, or broker routing failure |

---

## Inter-broker forwarding

When a request arrives at a broker that does not own the routing key, it is forwarded to the correct node. The forwarded
request includes:

- `X-Snapflux-Hop: 1` — prevents the target from re-routing
- `Authorization: Bearer <BROKER_AUTH_KEY>` — authenticates the forwarded request when auth is enabled

Clients do not need to be aware of routing and can send requests to any broker node.
