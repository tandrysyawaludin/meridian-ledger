# Operability

Day 4 gave Meridian Ledger the surfaces an operator needs: metrics to
scrape, logs to grep, a tracing seam to plug a backend into, a limiter
on the write path, and a shutdown sequence that does not drop committed
work. None of it changes the ledger semantics from Days 1–3.

## Metrics

`GET /metrics` on the HTTP port serves Prometheus text exposition, with
no authentication in this build — put it behind your usual scrape
network policy before exposing the port beyond localhost. Series:

| Series | Type | Meaning |
| --- | --- | --- |
| `meridian_postings_total{outcome}` | counter | Postings by outcome: `success` or `rejected`. Every Post call over HTTP or gRPC increments exactly one. |
| `meridian_posting_rejections_total{reason}` | counter | Rejections by reason: `unbalanced`, `idempotency_conflict`, `not_found`, `duplicate_account`, `validation`, `internal`. |
| `meridian_posting_duration_seconds` | histogram | Posting latency, including rejections. Buckets start at 500µs because the in-memory path posts in low microseconds. |
| `meridian_outbox_pending` | gauge | Unpublished outbox records — the relay's backlog. Sampled every 5s. |
| `meridian_outbox_oldest_age_seconds` | gauge | Age of the oldest unpublished outbox record. |

Two alerting notes. First, a spike in `unbalanced` almost always means
a buggy caller, not a ledger problem: the invariant held, the caller
sent a bad posting. Second, `outbox_pending` growing while
`outbox_oldest_age_seconds` stays small means a brief broker blip the
relay will drain; both growing together means the relay is stuck and
deserves a page.

One granularity caveat: the HTTP path reports the fine-grained domain
reason (`unbalanced`), while the gRPC path reports the coarser status
code reason (`validation`) — the handler converts domain errors to
status errors before the metrics interceptor sees them. Same series,
less detail on gRPC, never mislabelled.

The metrics package (`internal/metrics`) has no dependencies: the
exposition format is emitted by hand, keeping with the project's rule
that infrastructure dependencies must earn their place.

## Logs

All logs are JSON via `log/slog`, one line per event. Every HTTP
request logs one line on completion:

```json
{"level":"INFO","msg":"http request","request_id":"…","method":"POST",
 "path":"/transactions","status":201,"duration_ms":0}
```

`request_id` is the correlation ID: the middleware honors a
caller-supplied `X-Request-ID` (so a client can trace its own request
end to end) and generates a random one otherwise, echoing it in the
response header. gRPC calls log one analogous line per call with method
and status code. Relay poll failures log at WARN with the error — a
failed poll is retried on the next tick, but silently swallowing broker
outages would hide them from operators.

## Tracing

Tracing is a seam, not a backend. `internal/tracing` defines a small
`Tracer`/`Span` interface shaped like the OpenTelemetry API, and the
service ships with a no-op tracer: one span per HTTP request and per
gRPC call is created and ended, and nothing is exported. Wiring a real
backend is a one-line swap in `cmd/meridian` (implement the interface,
pass it in `api.Options` and the gRPC interceptors) — no call-site
refactor. The ledger domain takes no tracer: its operations are
synchronous and already covered by the request span.

## Rate limiting

`POST /transactions` is limited per client IP with a token bucket:
`POST_RATE_LIMIT_RPS` sustained rate (default 100/s) and
`POST_RATE_LIMIT_BURST` bucket size (default 200). Setting
`POST_RATE_LIMIT_RPS=0` disables limiting. Denied requests get HTTP 429
with a `Retry-After: 1` header and the standard JSON error shape
(`code: "rate_limited"`). Reads are not limited: balance and account
reads are cheap derived reads, and limiting them would punish
legitimate polling.

Two properties worth knowing. The limiter is per-instance, not
distributed: each service instance limits independently, which is the
right trade-off until the service runs behind shared state. And
`X-Forwarded-For` is deliberately ignored — the service does not know
whether it sits behind a trusted proxy, and trusting a
client-controlled header would let a caller rotate identities past the
limiter for free. Behind a proxy, limit at the proxy or trust its
header explicitly.

## Graceful shutdown

On SIGINT or SIGTERM the service shuts down in this order:

1. Stop accepting new work: HTTP shutdown drains in-flight requests,
   gRPC graceful stop drains in-flight calls (both bounded by a 10s
   timeout; gRPC falls back to a hard stop on timeout).
2. Stop the outbox lag reporter, then cancel the relay and wait for its
   in-flight poll (bounded by 10s, so a dead broker cannot hang
   shutdown forever).
3. Close the Kafka publisher, then the store.

The order is the point: a posting in flight when the signal arrives
still commits its outbox rows, and the relay is only stopped after the
servers have drained, so no committed event is left unpublished by a
relay killed mid-batch. Anything still unpublished after shutdown is
picked up by the next run, in outbox-sequence order — the at-least-once
contract from Day 3 covers the gap.

## Load testing

`cmd/loadtest` is a black-box harness: it posts over HTTP like a real
client, with a unique idempotency key per posting, and reports
throughput plus p50/p95/p99 latency and a breakdown of created,
rate-limited, and errored requests. Start the service first, then:

```sh
go run ./cmd/loadtest -duration 30s -concurrency 32
```

Published numbers (see README) are from the in-memory store on a dev
VM, single instance, localhost. They are a throughput ceiling, not a
capacity promise: Postgres numbers will be lower, and a load test from
one machine shares one rate-limit bucket per IP, so run with
`POST_RATE_LIMIT_RPS=0` when measuring raw throughput. Unit benchmarks
for the in-domain path live in `internal/ledger/ledger_bench_test.go`
(`go test -bench=. ./internal/ledger/`).
