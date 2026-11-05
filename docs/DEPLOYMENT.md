# Deployment

Day 5's shipping shape: a container image, a local full stack, and
Kubernetes manifests. The service itself is stateless — all durable state
lives in Postgres (journal + outbox) and Redpanda (events) — so the
deployment story is about wiring those three pieces together.

## Container image

Multi-stage `Dockerfile` at the repo root: a Go build stage compiles a
static binary, and the runtime stage is a distroless non-root image
containing only that binary. Ports: `8080` (HTTP) and `9090` (gRPC).

```sh
docker build -t meridian-ledger:latest .
docker run -p 8080:8080 -p 9090:9090 meridian-ledger:latest
```

Without `DATABASE_URL` the container runs on the in-memory store, which
is fine for a smoke test and useless for anything else — data does not
survive a restart. Point it at Postgres for real use.

## Local full stack (docker-compose)

`docker-compose.yml` starts the service, Postgres 16, and Redpanda:

```sh
docker compose up --build
curl localhost:8080/healthz
# {"service":"meridian-ledger","status":"ok"}
```

The service waits for Postgres to report healthy, then connects with
`DATABASE_URL=postgres://meridian:meridian@postgres:5432/meridian?sslmode=disable`
and publishes outbox events to Redpanda at `redpanda:9092`. The
credentials in the compose file are local-development defaults only.
Stop with `docker compose down -v` to also drop the volumes.

## Kubernetes

Manifests live in `deploy/k8s/`:

| File | Contents |
| --- | --- |
| `configmap.yaml` | Non-secret config: ports, broker address, topic, rate limits |
| `deployment.yaml` | One replica, probes, resource requests/limits, locked-down security context |
| `service.yaml` | ClusterIP exposing HTTP (8080) and gRPC (9090) |
| `hpa.yaml` | CPU/memory autoscaling shape (see the scaling caveat below) |

Apply in order, after creating the database Secret (credentials are
never checked in):

```sh
kubectl create secret generic meridian-ledger-db \
  --from-literal=database-url='postgres://USER:PASSWORD@HOST:5432/meridian?sslmode=require'
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
```

Probes hit `GET /healthz` (liveness) and `GET /readyz` (readiness) on
the HTTP port. The pod runs as non-root (UID 65532) with a read-only
root filesystem and all capabilities dropped. `/metrics` has no
authentication in this build — keep the Service ClusterIP-only or behind
a scrape network policy before exposing it.

### The one-replica rule, and the HPA caveat

`replicas` is 1 on purpose: the outbox relay runs in-process and is
documented as single-instance, because two relays draining the same
outbox could interleave batches and break per-account ordering. The
`hpa.yaml` manifest is therefore the scaling shape for *after* the relay
is extracted into its own deployment or given leadership — do not apply
it to this Deployment with `maxReplicas` above 1 as-is. Scaling the API
tier and scaling the relay are separate decisions; the API tier is
stateless and scales freely once the relay is out of the pod.

## Failure modes, tested

The failure-injection tests added on Day 5 (`internal/ledger/failure_test.go`,
`internal/events/failure_test.go`) cover the three scenarios an operator
most needs to trust:

- **Database dies mid-posting** — the posting fails outright, no
  half-written journal entry, no consumed idempotency key, and the
  identical retry succeeds once the database recovers. On Postgres this
  is structural (journal + outbox commit in one transaction), not luck.
- **Duplicate deliveries** — a broker redelivery after a lost
  acknowledgement is deduplicated; the handler runs once.
- **Consumer / relay crash and restart** — a restarted consumer backed
  by the same dedup store does not re-apply events, and a restarted
  relay drains the remaining outbox in order with no duplicates.

One honest limit is tested, not hidden: if a consumer crashes *after*
applying an event but *before* marking it processed, the redelivery
will apply it again. Delivery is at-least-once, so downstream handlers
must themselves be idempotent; the dedup store only suppresses
redeliveries it managed to record.
