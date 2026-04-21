# Authentication

Snapflux uses Bearer token authentication. When enabled, every request to the API must include a valid token in the `Authorization` header. The health check (`/health`) and metrics (`/metrics`) endpoints are always exempt.

---

## Enabling authentication

Set the `AUTH_KEYS` environment variable to a comma-separated list of `name:key` pairs:

```bash
AUTH_KEYS="billing-svc:s3cr3tkey1,inventory-svc:an0th3rkey"
```

Each name is an identifier used in logs and traces. Each key is the Bearer token that service must present.

When `AUTH_KEYS` is empty (the default), authentication is completely disabled with no performance overhead.

---

## Making authenticated requests

Include the key in the `Authorization` header:

```bash
curl -X POST http://snapflux:5050/v1/topics/orders/messages \
  -H "Authorization: Bearer s3cr3tkey1" \
  -H "Content-Type: application/json" \
  -d '{"key": "order-1", "body": {}}'
```

A missing or invalid token returns `401 Unauthorized`:

```json
{"error": "unauthorized"}
```

---

## Inter-broker authentication

In a multi-broker deployment, brokers forward requests to each other using their own credential. Set `BROKER_AUTH_KEY` to a separate token:

```bash
BROKER_AUTH_KEY="internal-broker-secret"
```

This key is validated independently of `AUTH_KEYS` — it does not need to appear in the client key list. Brokers automatically include it in all forwarded requests.

In practice this means each broker Pod needs both variables set:

```yaml
env:
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
```

---

## Storing keys in Kubernetes

Never put raw API keys in a `Deployment` manifest. Use a `Secret`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: snapflux-secrets
type: Opaque
stringData:
  auth-keys: "billing-svc:aHR0cHM6Ly9leGFtcGxlLmNvbQ,inventory-svc:bG9yZW1pcHN1bWRvbG9y"
  broker-auth-key: "aW50ZXJuYWxicm9rZXJrZXkxMjM0NTY"
```

Generate secure random keys with:

```bash
openssl rand -base64 32
```

---

## Key rotation

Snapflux reads `AUTH_KEYS` at startup. To rotate a key without downtime:

1. Add the new key alongside the old one in `AUTH_KEYS` (e.g. `billing-svc:oldkey,billing-svc-new:newkey`).
2. Deploy the updated Secret and roll the Snapflux Pods.
3. Update all clients to use the new key.
4. Remove the old key from `AUTH_KEYS` and redeploy.

---

## Security notes

- Keys are compared with standard string equality. Use sufficiently long, randomly generated keys (at minimum 32 bytes of entropy).
- The `BROKER_AUTH_KEY` should be different from all client keys so that a compromised client key cannot be used for broker-to-broker impersonation.
- Snapflux does not currently support key expiry or scoped permissions per topic. If you need fine-grained access control, enforce it at the ingress layer (e.g. an API gateway or service mesh policy).
- Run Snapflux behind TLS termination (an ingress or service mesh sidecar) so that tokens are not transmitted in plaintext.
