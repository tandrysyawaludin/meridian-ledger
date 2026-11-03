package events

import (
	"context"
	"fmt"
	"time"
)

// OutboxRecord is one unpublished (or, historically, published) event in
// the outbox table. Seq is the store-assigned, monotonically increasing
// sequence: the relay publishes in Seq order, and that order is the whole
// basis of the per-account ordering guarantee, since a single account's
// events are a subsequence of the global sequence.
type OutboxRecord struct {
	Seq   int64
	Event Event
}

// OutboxStore is the port the relay uses to drain the outbox. Both the
// in-memory and the Postgres stores implement it. PendingOutbox must
// return unpublished records in ascending Seq order, capped at limit.
type OutboxStore interface {
	PendingOutbox(ctx context.Context, limit int) ([]OutboxRecord, error)
	MarkOutboxPublished(ctx context.Context, seq int64) error
	MarkOutboxAttempt(ctx context.Context, seq int64) error
}

// Publisher is the port the relay publishes through. The Kafka
// implementation lives in kafka.go; tests use a recording fake.
type Publisher interface {
	Publish(ctx context.Context, event Event) error
	Close() error
}

// Relay drains the outbox in sequence order and publishes each event.
//
// Ordering is preserved by head-of-line blocking: if publishing one event
// fails, the relay records the attempt, stops the batch, and returns the
// error, leaving that event and everything after it unpublished for the
// next poll. Reordering around a failed event would break the per-account
// guarantee, so the relay never does it. A failed event is retried on the
// next poll; because delivery is at-least-once, a publish that succeeded
// on the broker but whose acknowledgement was lost is simply redelivered,
// and the consumer deduplicates it.
//
// The relay is intended to run as a single instance. Two relays draining
// the same outbox would still be correct — publishing is idempotent at
// the consumer — but they could interleave batches and break ordering,
// so do not run more than one.
type Relay struct {
	Store        OutboxStore
	Publisher    Publisher
	BatchSize    int           // events per poll; default 100
	PollInterval time.Duration // delay between polls in Run; default 1s
}

// batchSize returns the configured batch size, or the default.
func (r *Relay) batchSize() int {
	if r.BatchSize <= 0 {
		return 100
	}
	return r.BatchSize
}

// RunOnce publishes one batch of pending events, in sequence order. It
// returns how many events were published. On a publish failure it returns
// the count published before the failure together with the error; the
// failed event stays unpublished for a later poll.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	records, err := r.Store.PendingOutbox(ctx, r.batchSize())
	if err != nil {
		return 0, fmt.Errorf("events: list pending outbox: %w", err)
	}
	published := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		if err := r.Publisher.Publish(ctx, record.Event); err != nil {
			// Record the attempt, then stop: reordering past a failed
			// event would break per-account ordering.
			if markErr := r.Store.MarkOutboxAttempt(ctx, record.Seq); markErr != nil {
				return published, fmt.Errorf("events: publish %s failed (%v) and recording the attempt failed: %w", record.Event.ID, err, markErr)
			}
			return published, fmt.Errorf("events: publish %s: %w", record.Event.ID, err)
		}
		if err := r.Store.MarkOutboxPublished(ctx, record.Seq); err != nil {
			// The event may already be on the broker; the consumer's
			// deduplication makes a later redelivery harmless.
			return published, fmt.Errorf("events: mark %s published: %w", record.Event.ID, err)
		}
		published++
	}
	return published, nil
}

// Run polls the outbox until ctx is cancelled. A poll error is not fatal:
// the failed event stays unpublished, so nothing is lost and ordering is
// preserved, and the next tick retries it. Callers that want poll errors
// surfaced should drive RunOnce in their own loop instead.
func (r *Relay) Run(ctx context.Context) {
	interval := r.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		_, _ = r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
