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

- [ ] HTTP API: create account, post transaction, get balance, get transaction
- [ ] gRPC API with protobuf definitions mirroring the HTTP surface
- [ ] Postgres store with pgx: schema migrations, posting in a single DB
      transaction, unique constraint on idempotency keys
- [ ] Optimistic concurrency for balance snapshots
- [ ] Integration tests against a real Postgres (testcontainers or compose)

## Day 3 — Event streaming

- [ ] Transactional outbox: journal writes and outbox rows commit atomically
- [ ] Outbox relay publishing to Kafka/Redpanda
- [ ] Reference consumer with deduplication and dead-letter handling
- [ ] Ordering guarantees per account documented and tested

## Day 4 — Operability and load

- [ ] Prometheus metrics: posting latency, rejections by reason, outbox lag
- [ ] Structured logging throughout, request-scoped log fields
- [ ] Tracing hooks (OpenTelemetry interfaces, no-op by default)
- [ ] Rate limiting on the posting API
- [ ] Graceful shutdown for HTTP, gRPC and the outbox relay
- [ ] Load-test harness; benchmark numbers published in the README

## Day 5 — Shipping shape

- [ ] Multi-stage Dockerfile and a slim runtime image
- [ ] Kubernetes manifests: Deployment, Service, ConfigMap, probes, HPA
- [ ] docker-compose stack: service + Postgres + Redpanda for local runs
- [ ] Failure-injection tests: killed DB mid-posting, duplicate deliveries,
      consumer crash/restart
- [ ] README and results polish; end-to-end walkthrough
