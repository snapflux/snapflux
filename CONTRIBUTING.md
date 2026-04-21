# Contributing to Snapflux

Thank you for your interest in contributing. This document covers how to set up a development environment, submit
changes, and navigate the review process.

---

## Code of Conduct

This project follows the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/main/code-of-conduct.md). By
participating you agree to abide by its terms.

---

## Reporting bugs

Open a [GitHub issue](../../issues/new) with:

- A clear description of the problem
- Steps to reproduce
- Expected vs. actual behaviour
- Snapflux version, Go version, and storage backend in use

For security vulnerabilities, do **not** open a public issue. See [SECURITY.md](SECURITY.md).

---

## Development setup

**Prerequisites:** Go 1.25+, Docker (optional, for running MongoDB locally)

```bash
# Clone the repository
git clone https://github.com/snapflux/snapflux.git
cd snapflux

# Install dependencies
go mod download

# Run with in-memory storage (no external dependencies)
go run ./cmd/api/main.go

# Run with MongoDB
docker run -d -p 27017:27017 mongo:7
DATABASE_URL=mongodb://localhost:27017 go run ./cmd/api/main.go
```

---

## Project layout

```
cmd/api/          Entry point
internal/
  auth/           Bearer token authentication middleware
  broker/         Self-registration, heartbeat, consistent hash ring
  config/         Environment variable loading
  messaging/      Send / receive / ack logic, BatchFlush, Requeue
  model/          Request and response types
  server/         HTTP handlers and routing
  storage/        Provider interface; mongodb/ and memory/ backends
  telemetry/      OpenTelemetry setup (metrics + traces)
  wal/            Write-ahead log
clients/
  nestjs/         Official NestJS client library
docs/             Project documentation
examples/         Integration examples
```

---

## Making changes

1. Fork the repository and create a branch from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```

2. Make your changes. Keep commits focused — one logical change per commit.

3. Ensure the code builds and passes linting:
   ```bash
   go build ./...
   go vet ./...
   ```

4. Add or update tests as appropriate.

5. Open a pull request against `main`. Fill in the PR template.

---

## Coding conventions

- Follow standard Go idioms and the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).
- All configuration must be loaded via environment variables in `internal/config`. No config files.
- Storage operations belong behind the `storage.Provider` interface — never call a specific backend directly from
  outside the `storage/` package.
- New instrumentation (metrics, spans) should follow the naming conventions in `docs/observability.md`.
- Comments explain *why*, not *what*. Avoid restating what the code already says.

---

## Adding a storage backend

1. Create a new package under `internal/storage/<name>/`.
2. Implement the `storage.Provider` interface defined in `internal/storage/storage.go`.
3. Wire it into `cmd/api/main.go` behind an appropriate environment variable check.
4. Document it in `docs/configuration.md` and `docs/architecture.md`.

---

## Pull request guidelines

- Keep PRs small and focused. Large refactors are harder to review.
- Link the relevant issue if one exists.
- All checks must pass before merge.
- At least one maintainer approval is required.

---

## Licensing

By submitting a contribution you agree that your work will be licensed under
the [GNU Affero General Public License v3.0](LICENSE).
