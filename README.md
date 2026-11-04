# Meridian Ledger

A production-grade, idempotent **double-entry payments ledger** service in Go.

Meridian Ledger is the bookkeeping core you would put underneath a payments
platform: every movement of money is recorded as a balanced set of debits and
credits in an append-only journal, balances are always derived from that
journal, and every posting is safe to retry. It is built to demonstrate the
properties a real ledger must have — correctness first, then throughput —
rather than to be a CRUD demo.

> Status: under active development. See [ROADMAP.md](ROADMAP.md) for the
> build plan and what has landed so far.

## Why this exists

Payment systems fail in boring, expensive ways: a retried request charges a
customer twice, a partial write leaves the books unbalanced, a float rounds
a balance into fiction. Meridian Ledger is designed around eliminating those
failure classes:

- **Correctness** — the balanced-books invariant (total debits == total
  credits) is enforced in the domain layer on every posting. An unbalanced
  transaction cannot be persisted, by construction.
- **Idempotency** — every posting carries a caller-supplied idempotency key.
  Retries return the original transaction instead of double-applying, and
  re-using a key with a different payload is rejected as a conflict.
- **Auditability** — the journal is append-only and immutable. Balances are
  derived, never stored as a mutable number that can drift from history.
  Corrections happen through reversing entries, exactly as in real
  bookkeeping.
- **No float money** — all amounts are integer minor units (cents, sen,
  fils). There is not a single float in the money path.

## Architecture

```mermaid
flowchart LR
    Client([API client]) -->|HTTP / gRPC| API[API layer<br/>cmd/meridian]
    API --> SVC[Ledger service<br/>internal/ledger]
    SVC -->|validates invariants<br/>+ idempotency| SVC
    SVC --> Repo[(Repository port)]
    Repo --> Mem[In-memory store<br/>tests / local dev]
    Repo --> PG[(Postgres store<br/>pgx)]
    Repo --> Outbox[(Transactional outbox<br/>same DB tx as journal)]
    Outbox --> Relay[Outbox relay<br/>sequence order]
    Relay -->|Kafka / Redpanda<br/>keyed by account| Stream[Event stream]
    Stream --> Consumers[Downstream consumers<br/>dedup + DLQ]
    SVC -.-> Obs[Metrics / logs / traces<br/>GET /metrics, JSON logs, tracing seam]
```

The domain package (`internal/ledger`) has no dependency on any storage or
transport technology. Storage sits behind a small `Repository` interface, so
the in-memory store used in tests and the Postgres store used in production
exercise exactly the same invariant and idempotency logic.

## Layout

```
cmd/meridian/        Service entry point (HTTP + gRPC servers, health endpoints,
                     /metrics, coordinated shutdown)
cmd/loadtest/        Black-box load-test harness (HTTP postings, latency percentiles)
internal/ledger/     Double-entry domain: accounts, entries, transactions,
                     invariant + idempotency enforcement, balance derivation,
                     versioned balance snapshots
internal/store/      Store interface, in-memory implementation, pgx Postgres
                     implementation (schema in internal/store/schema.sql),
                     transactional outbox in both backends
internal/events/     Event model (one event per account leg), outbox relay
                     to Kafka/Redpanda, reference consumer with dedup + DLQ
internal/api/        HTTP JSON API (same contract as the gRPC surface),
                     request-ID / logging / tracing middleware, posting rate limiter
internal/grpcapi/    gRPC adapter over the generated protobuf service,
                     logging / tracing / metrics interceptors
internal/metrics/    Prometheus exposition (dependency-free): posting latency,
                     rejections by reason, outbox lag gauges
internal/tracing/    Tracing seam: Tracer/Span interfaces, no-op default
gen/ledger/v1/       Generated protobuf + gRPC code (from proto/ledger/v1/)
proto/ledger/v1/     Protobuf service definition (source of truth)
```

## Key design decisions

1. **Integer minor units everywhere.** Amounts are `int64` counts of the
   currency's smallest unit. This removes an entire class of rounding bugs
   and makes equality checks — the heart of the balanced invariant — exact.
2. **Balances are derived, not stored.** A balance is a fold over the
   account's journal entries. It can never disagree with history, and any
   point-in-time balance can be reconstructed by folding a prefix of the
   journal.
3. **Idempotency is a domain concern, not a transport concern.** Keys are
   checked inside the posting operation (and, in Postgres, backed by a
   unique constraint), so HTTP retries, gRPC retries and queue redeliveries
   are all safe.
4. **One currency per transaction.** Cross-currency movement is modelled as
   two balanced transactions joined by an explicit FX step, never as a
   single transaction that silently mixes currencies.
5. **Append-only journal.** Stored entries are deep-copied on write and
   never mutated. Reversals are new transactions, preserving a complete
   audit trail.
6. **Standard library first.** The Day 1 core has zero external
   dependencies. Infrastructure dependencies (pgx, Kafka client) are added
   only where they earn their place, behind interfaces.

## Running it

Requires Go 1.24+ (developed against the latest stable Go).

```sh
go build ./...
go test ./...

# Start the service (in-memory store; HTTP on :8080, gRPC on :9090)
go run ./cmd/meridian

curl localhost:8080/healthz
# {"service":"meridian-ledger","status":"ok"}
```

Configuration: `PORT` sets the HTTP listen port (default `8080`),
`GRPC_PORT` the gRPC port (default `9090`), and `DATABASE_URL` selects
the Postgres store (a Postgres connection string). When `DATABASE_URL`
is unset, the in-memory store is used. `KAFKA_BROKERS` (comma-separated)
starts the outbox relay publishing to `KAFKA_TOPIC` (default
`meridian-ledger-events`); without it, events accumulate unpublished in
the outbox and a later brokered run publishes them in order. See
[`docs/EVENTS.md`](docs/EVENTS.md) for the delivery contract: at-least-once
delivery with consumer deduplication, per-account ordering, and
dead-letter handling. `POST_RATE_LIMIT_RPS` (default `100`) and
`POST_RATE_LIMIT_BURST` (default `200`) configure per-IP rate limiting
on the posting API (`0` disables it). `GET /metrics` serves Prometheus
metrics. Full operability detail — series, log fields, tracing seam,
shutdown order — is in [`docs/OPERABILITY.md`](docs/OPERABILITY.md).

### Event streaming

Every posting is fanned out into one `ledger.transaction.posted.v1`
event per account it touches, keyed by that account so each account's
events stay ordered on the broker. The outbox rows commit in the same
database transaction as the journal write, so an event can never exist
without its transaction. Downstream, the reference consumer in
`internal/events` deduplicates on the deterministic event ID, retries a
failing handler (default 3 attempts), then dead-letters the event instead
of looping on it.

### API

HTTP (JSON, under `/api`):

```sh
curl -X POST localhost:8080/api/accounts \
  -d '{"id":"cash","name":"Cash","currency":"USD"}'
curl -X POST localhost:8080/api/transactions \
  -d '{"idempotency_key":"sale-1","memo":"card sale","entries":[
        {"account_id":"cash","direction":"debit","amount_minor":5000},
        {"account_id":"revenue","direction":"credit","amount_minor":5000}]}'
curl localhost:8080/api/accounts/cash/balance
# {"account_id":"cash","balance_minor":5000,"currency":"USD","version":1}
```

The balance response carries a `version` that increments on every posting
touching the account: a snapshot read at version V is stale as soon as the
version moves, which is the optimistic-concurrency signal for readers.

The gRPC surface mirrors the HTTP API exactly; see
[`proto/ledger/v1/ledger.proto`](proto/ledger/v1/ledger.proto). Regenerate
the Go bindings after editing the proto file with:

```sh
protoc -I proto \
  --go_out=gen --go_opt=paths=source_relative \
  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
  proto/ledger/v1/ledger.proto
```

### Benchmarks

Measured 2026-11-04 on the dev VM (AMD EPYC, in-memory store, single
instance, localhost client). These are a throughput ceiling for the
in-memory path, not a capacity promise — Postgres numbers will be
lower, and production capacity needs a test against the real topology.

**HTTP load test** (`cmd/loadtest`, 16 workers, 10s, rate limiting
disabled, every posting a unique idempotency key):

```
loadtest: 65902 requests in 10.05s (6556.6 req/s)
  created: 65902  rate-limited: 0  errors: 0
  latency: p50=1.48ms p95=5.01ms p99=31.52ms max=89.48ms
```

The p99 tail is scheduling and GC under contention, not the posting
path itself: the in-domain benchmark below posts in under 4µs. The
service serializes postings on one mutex (the idempotency
check-and-insert), so throughput here is effectively single-core
posting throughput plus HTTP overhead — sharding that lock is the
obvious next scaling step if throughput ever becomes the constraint.

**In-domain benchmarks** (`go test -bench=. ./internal/ledger/`,
journal of 1,000 postings for the snapshot benchmark):

```
BenchmarkPost-4               379604    3771 ns/op
BenchmarkBalanceSnapshot-4    143743    8362 ns/op
```

With default rate limiting on (100 req/s, burst 200 per IP), the same
16-worker load test from one machine — one shared IP bucket — yielded
700 created and 34,702 rate-limited requests with 0 errors, which is
the limiter doing exactly what it is for.

### Integration tests

The Postgres integration test runs against a real database and is skipped
unless `MERIDIAN_TEST_DATABASE_URL` points at a scratch database (the
test truncates the ledger tables, so never point it at real data):

```sh
MERIDIAN_TEST_DATABASE_URL=postgres://postgres@localhost:5432/meridian_test \
  go test ./internal/store/ -run TestPostgresStoreIntegration -v
```

## Roadmap summary

- **Day 1** — Scaffold, double-entry core with invariant + idempotency
  enforcement, in-memory store, unit tests, CI. ✅
- **Day 2** — HTTP + gRPC APIs, Postgres store (pgx), optimistic
  concurrency, integration tests. ✅
- **Day 3** — Transactional outbox → Kafka/Redpanda streaming, consumers
  with deduplication and dead-letter handling. ✅
- **Day 4** — Observability (Prometheus metrics, structured logs, tracing
  hooks), rate limiting, graceful shutdown, load-test harness with published
  benchmark numbers. ✅
- **Day 5** — Docker + Kubernetes manifests, docker-compose full stack,
  failure-injection tests, results polish.

Full detail in [ROADMAP.md](ROADMAP.md).

## License

MIT — see [LICENSE](LICENSE).
