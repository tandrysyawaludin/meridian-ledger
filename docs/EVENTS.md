# Event streaming (Day 3)

Every posted transaction is published to a Kafka/Redpanda topic as one
event per account it touches. This document states the delivery contract
precisely, because streaming guarantees are where ledger systems most
often over-promise.

## The event

- **Type:** `ledger.transaction.posted.v1` (schema version in the suffix).
- **ID:** `<transaction_id>:<account_id>` — deterministic, so a redelivery
  carries the same ID as the original and deduplicates cleanly.
- **Partition key:** the leg's account. The Kafka publisher keys messages
  on it with a hash balancer, so all events for one account land in one
  partition.
- **Payload:** the full transaction (entries in integer minor units, memo,
  posting time) plus the leg's account. A consumer can project balances
  without a second read.

## How a posting becomes an event

1. `PostgresStore.SaveTransaction` inserts the transaction row, its
   entries, the per-account version bumps, and one outbox row per distinct
   account — all in a **single database transaction**. A crash anywhere
   in that function leaves either the posting and its events, or neither.
   The in-memory store holds the same lock across the journal write and
   the outbox append, giving the same atomicity for tests and local runs.
2. The relay (`internal/events.Relay`) polls the outbox in ascending
   sequence order and publishes each event. On success it stamps the row
   published; on failure it records the attempt and stops the batch.
3. Downstream, the reference consumer (`internal/events.Consumer`)
   deduplicates on event ID, retries a failing handler up to `MaxAttempts`
   (default 3), then dead-letters the event instead of looping on it
   forever. A dead-lettered event can be reprocessed with `Replay` once
   the underlying problem is fixed.

## Guarantees

| Guarantee | How it holds | What it does not promise |
| --- | --- | --- |
| **No lost events** | Outbox rows commit with the journal write; unpublished rows are retried until the broker acknowledges. | If the database itself is lost, its outbox goes with it — this is a durability property of the store, not of the relay. |
| **No duplicate effects** | At-least-once delivery plus consumer deduplication on the deterministic event ID. | Delivery itself is at-least-once, not exactly-once: a consumer may *receive* a duplicate. It must not *apply* it twice, and the reference consumer guarantees that for redeliveries arriving after a successful processing. A crash between applying and marking processed can still double-apply; handlers with side effects should be idempotent themselves, or mark processed in the same transaction as their effects. |
| **Per-account ordering** | The relay publishes in global sequence order from a single instance, and partitioning by account keeps each account's events in one partition, in that order. A consumer processing sequentially (see `Consumer.Serve`) therefore sees each account's postings in journal order. | There is **no** global ordering across accounts, and none is promised: two accounts' interleaving on the topic is not meaningful. Running more than one relay instance preserves correctness (deduplication still holds) but breaks ordering, so run exactly one. |
| **Head-of-line blocking** | A publish failure stops the batch rather than skipping ahead, which is what keeps the ordering guarantee intact. | A permanently unpublishable event (e.g. a poison payload) blocks later events behind it. That is the deliberate trade-off for ordering; the remedy is to fix the publisher or the payload, not to skip. Consumer-side poison events, by contrast, do not block: they dead-letter and the stream continues. |

## Configuration

- `KAFKA_BROKERS` — comma-separated `host:port` list. When unset, the
  relay does not run; events accumulate unpublished in the outbox and a
  later run with a broker publishes them in order.
- `KAFKA_TOPIC` — topic name (default `meridian-ledger-events`).
- Publisher acknowledgements are `RequireAll`: an event counts as
  published only once every in-sync replica has it.

## Local trial (Redpanda)

Day 5 adds a docker-compose stack with Redpanda. Until then, point
`KAFKA_BROKERS` at any reachable Kafka/Redpanda broker and watch the
topic; a consumer can be built on `events.Consumer` with a
`MemoryDedupStore` for a first projection, backed by a durable dedup
table before production use.
