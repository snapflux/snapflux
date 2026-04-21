# Security Policy

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security issues by emailing **support@vinr.eu**. Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Any suggested mitigations

You will receive an acknowledgement within 48 hours. We aim to release a fix within 90 days of initial disclosure,
depending on severity and complexity. We will coordinate the disclosure timeline with you.

---

## Supported versions

| Version       | Security fixes |
|---------------|----------------|
| latest `main` | Yes            |

---

## Security model

### Authentication

API authentication is opt-in via the `AUTH_KEYS` environment variable. When not set, **all requests are accepted without
credentials** — this is appropriate for deployments where network-level controls (Kubernetes `NetworkPolicy`, a service
mesh, or an ingress with authentication) handle access control.

When enabled:

- Client API keys and the broker-to-broker key (`BROKER_AUTH_KEY`) are validated independently.
- Keys are compared using standard string equality. There is no built-in rate limiting on failed authentication
  attempts; enforce this at the ingress or load balancer layer.
- There is no built-in key expiry or per-topic scope — access is all-or-nothing per key.

### Transport security

Snapflux does not terminate TLS itself. Run it behind a TLS-terminating ingress or sidecar proxy (e.g. Envoy, Nginx, or
a service mesh). Tokens and message payloads should not be transmitted over plaintext HTTP in production.

### Inter-broker communication

Broker-to-broker forwarding uses the `BROKER_AUTH_KEY` credential. Brokers should communicate over a private Kubernetes
network, not exposed externally.

### Storage credentials

The `DATABASE_URL` (MongoDB connection string) is passed as an environment variable. Store it in a Kubernetes `Secret`,
not in a `ConfigMap` or image.

### WAL files

The write-ahead log at `WAL_PATH` contains message payloads in plaintext. Use encrypted storage volumes if message
content is sensitive.

---

## Known limitations

- No per-topic or per-consumer-group access control.
- No built-in TLS.
- No audit log for authentication events.
- WAL files are not encrypted at rest.

These are acknowledged design trade-offs for the current version.
