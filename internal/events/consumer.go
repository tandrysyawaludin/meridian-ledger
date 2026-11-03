package events

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Handler processes one event. Returning an error means the event was not
// applied; the consumer retries it (up to MaxAttempts) and then
// dead-letters it. A handler must itself be idempotent in the applied
// sense — it may be called more than once for the same event if the
// process crashes between applying and marking processed — but the
// consumer's deduplication guarantees it is not called twice for a
// redelivery that arrives after a successful processing.
type Handler func(ctx context.Context, event Event) error

// DeadLetter is an event that exhausted its processing attempts. It is
// kept, with the last error, so an operator (or Replay) can inspect and
// reprocess it instead of it vanishing or looping forever.
type DeadLetter struct {
	Event          Event
	Err            string
	Attempts       int
	DeadLetteredAt time.Time
}

// DedupStore is the consumer's record of what has been processed and
// what has been dead-lettered. The in-memory implementation is the
// reference; a production consumer would back this with a table keyed by
// event ID, updated in the same transaction as the handler's side effects
// where exactly-once effects matter.
type DedupStore interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
	RecordDeadLetter(ctx context.Context, dl DeadLetter) error
	DeadLetters(ctx context.Context) ([]DeadLetter, error)
	RemoveDeadLetter(ctx context.Context, eventID string) error
}

// Consumer is the reference downstream consumer: deduplication, bounded
// retries, and dead-letter handling. It processes events sequentially
// (see Serve), so per-partition order is preserved end to end.
type Consumer struct {
	Handler     Handler
	Dedup       DedupStore
	MaxAttempts int // handler attempts before dead-lettering; default 3
}

// maxAttempts returns the configured attempt budget, or the default.
func (c *Consumer) maxAttempts() int {
	if c.MaxAttempts <= 0 {
		return 3
	}
	return c.MaxAttempts
}

// Process handles one event.
//
//   - If the event was already processed (or already dead-lettered), it is
//     a redelivery and is skipped without calling the handler.
//   - Otherwise the handler is tried up to MaxAttempts times. On success
//     the event is marked processed.
//   - If every attempt fails, the event is dead-lettered and marked
//     processed, so a later redelivery does not restart the poison loop;
//     the dead-letter record is the recovery path (see Replay). Process
//     returns an error in this case so the caller can log it.
func (c *Consumer) Process(ctx context.Context, event Event) error {
	processed, err := c.Dedup.IsProcessed(ctx, event.ID)
	if err != nil {
		return fmt.Errorf("events: check processed %s: %w", event.ID, err)
	}
	if processed {
		return nil
	}
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts(); attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.Handler(ctx, event); err == nil {
			if err := c.Dedup.MarkProcessed(ctx, event.ID); err != nil {
				return fmt.Errorf("events: mark processed %s: %w", event.ID, err)
			}
			return nil
		} else {
			lastErr = err
		}
	}
	dl := DeadLetter{
		Event:          event,
		Err:            lastErr.Error(),
		Attempts:       c.maxAttempts(),
		DeadLetteredAt: time.Now().UTC(),
	}
	if err := c.Dedup.RecordDeadLetter(ctx, dl); err != nil {
		return fmt.Errorf("events: record dead letter %s: %w", event.ID, err)
	}
	// Mark processed so redeliveries of this poison event are skipped;
	// the dead-letter record, not a retry loop, is where it waits.
	if err := c.Dedup.MarkProcessed(ctx, event.ID); err != nil {
		return fmt.Errorf("events: mark dead-lettered %s processed: %w", event.ID, err)
	}
	return fmt.Errorf("events: event %s dead-lettered after %d attempts: %w", event.ID, c.maxAttempts(), lastErr)
}

// Replay reprocesses one dead-lettered event directly, bypassing the
// processed check (a dead-lettered event is marked processed by design).
// On success the dead-letter record is removed; on failure it stays, with
// its original error, for another attempt later.
func (c *Consumer) Replay(ctx context.Context, eventID string) error {
	letters, err := c.Dedup.DeadLetters(ctx)
	if err != nil {
		return fmt.Errorf("events: list dead letters: %w", err)
	}
	for _, dl := range letters {
		if dl.Event.ID != eventID {
			continue
		}
		if err := c.Handler(ctx, dl.Event); err != nil {
			return fmt.Errorf("events: replay %s: %w", eventID, err)
		}
		if err := c.Dedup.RemoveDeadLetter(ctx, eventID); err != nil {
			return fmt.Errorf("events: clear dead letter %s: %w", eventID, err)
		}
		return nil
	}
	return fmt.Errorf("events: no dead letter for event %s", eventID)
}

// Serve processes events from ch sequentially until ch closes or ctx is
// cancelled. A dead-lettered event does not stop the stream: Process's
// error is swallowed after the event is recorded, so later events —
// including later events for the same account — still flow. Sequential
// processing is what preserves the order the relay and the broker
// established.
func (c *Consumer) Serve(ctx context.Context, ch <-chan Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			_ = c.Process(ctx, event)
		}
	}
}

// MemoryDedupStore is a goroutine-safe, in-memory DedupStore: the
// reference implementation for tests and local development.
type MemoryDedupStore struct {
	mu        sync.Mutex
	processed map[string]bool
	dead      []DeadLetter
}

// NewMemoryDedupStore returns an empty deduplication store.
func NewMemoryDedupStore() *MemoryDedupStore {
	return &MemoryDedupStore{processed: make(map[string]bool)}
}

// IsProcessed reports whether eventID has been processed or dead-lettered.
func (m *MemoryDedupStore) IsProcessed(_ context.Context, eventID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processed[eventID], nil
}

// MarkProcessed records eventID as processed.
func (m *MemoryDedupStore) MarkProcessed(_ context.Context, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed[eventID] = true
	return nil
}

// RecordDeadLetter appends a dead-letter record.
func (m *MemoryDedupStore) RecordDeadLetter(_ context.Context, dl DeadLetter) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dead = append(m.dead, dl)
	return nil
}

// DeadLetters returns a copy of the dead-letter records, in arrival order.
func (m *MemoryDedupStore) DeadLetters(_ context.Context) ([]DeadLetter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DeadLetter(nil), m.dead...), nil
}

// RemoveDeadLetter drops the dead-letter record for eventID, if any.
func (m *MemoryDedupStore) RemoveDeadLetter(_ context.Context, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.dead[:0]
	for _, dl := range m.dead {
		if dl.Event.ID != eventID {
			kept = append(kept, dl)
		}
	}
	m.dead = kept
	return nil
}

var _ DedupStore = (*MemoryDedupStore)(nil)
