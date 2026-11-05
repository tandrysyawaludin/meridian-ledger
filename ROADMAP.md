# Roadmap

Meridian Ledger is being built in five focused days. Each day ends with a
working, tested system — no "big bang" integration at the end.

## Day 1 — Core ledger (2026-11-01)

- [x] Go module scaffold (`cmd/`, `internal/ledger`, `internal/store`)
- [x] Double-entry domain model: accounts, entries, transactions
- [x] Balanced-books invariant enforced on every posting (debits == credits)
- [x] Integer minor units for all amounts — no floats in the money path
- [x] Idempotency keys on postings: safe retries, conflict on payload reuse
- [x] Single-currency-per-transaction rule
- [x] Balances derived from the append-only journal
- [x] Store interface with a working in-memory implementation
- [x] Postgres store stub satisfying the same interface
- [x] Unit tests: balanced posting, unbalanced rejection, idempotent repost,
      balance across multiple postings, validation matrix
- [x] Runnable service skeleton with `/healthz` and `/readyz`
- [x] GitHub Actions CI (`go vet`, `go test`, `go build`, gofmt check)

## Day 2 — APIs and real persistence

- [x] HTTP API: create account, post transaction, get balance, get transaction
- [x] gRPC API with protobuf definitions mirroring the HTTP surface
- [x] Postgres store with pgx: schema migrations, posting in a single DB
      transaction, unique constraint on idempotency keys
- [x] Optimistic concurrency for balance snapshots
- [x] Integration tests against a real Postgres (testcontainers or compose)

## Day 3 — Event streaming

- [x] Transactional outbox: journal writes and outbox rows commit atomically
- [x] Outbox relay publishing to Kafka/Redpanda
- [x] Reference consumer with deduplication and dead-letter handling
- [x] Ordering guarantees per account documented and tested

## Day 4 — Operability and load

- [x] Prometheus metrics: posting latency, rejections by reason, outbox lag
- [x] Structured logging throughout, request-scoped log fields
- [x] Tracing hooks (OpenTelemetry interfaces, no-op by default)
- [x] Rate limiting on the posting API
- [x] Graceful shutdown for HTTP, gRPC and the outbox relay
- [x] Load-test harness; benchmark numbers published in the README

## Day 5 — Shipping shape

- [x] Multi-stage Dockerfile and a slim runtime image
- [x] Kubernetes manifests: Deployment, Service, ConfigMap, probes, HPA
- [x] docker-compose stack: service + Postgres + Redpanda for local runs
- [x] Failure-injection tests: killed DB mid-posting, duplicate deliveries,
      consumer crash/restart
- [x] README and results polish; end-to-end walkthrough
