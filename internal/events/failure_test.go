package events_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// Day 5 failure-injection tests: duplicate deliveries, consumer
// crash/restart, and relay crash/restart. The delivery contract under
// test is at-least-once with consumer deduplication (see docs/EVENTS.md):
// redelivery must never double-apply, and a restart must resume from
// durable state, not from scratch.

func testEvent() events.Event {
	return events.Event{
		ID:            "tx-1:cash",
		Type:          "ledger.transaction.posted.v1",
		TransactionID: "tx-1",
		AccountID:     "cash",
		Entries: []events.EventEntry{
			{AccountID: "cash", Direction: "debit", AmountMinor: 5000},
			{AccountID: "revenue", Direction: "credit", AmountMinor: 5000},
		},
	}
}

// TestDuplicateDeliveriesApplyOnce delivers the same event twice to one
// consumer — the shape of a broker redelivery after a lost acknowledgement
// — and requires the handler to run exactly once.
func TestDuplicateDeliveriesApplyOnce(t *testing.T) {
	ctx := context.Background()
	var applied atomic.Int32
	consumer := &events.Consumer{
		Handler: func(context.Context, events.Event) error {
			applied.Add(1)
			return nil
		},
		Dedup: events.NewMemoryDedupStore(),
	}
	event := testEvent()
	if err := consumer.Process(ctx, event); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if err := consumer.Process(ctx, event); err != nil {
		t.Fatalf("duplicate delivery should be skipped, got error: %v", err)
	}
	if applied.Load() != 1 {
		t.Errorf("handler applied %d times; want 1", applied.Load())
	}
}

// TestConsumerCrashRestartDoesNotRedeliver processes an event, discards
// the consumer (the crash), then builds a fresh consumer over the same
// deduplication store (the restart) and redelivers the event. The restart
// must not re-apply it, and must still process genuinely new events.
func TestConsumerCrashRestartDoesNotRedeliver(t *testing.T) {
	ctx := context.Background()
	var applied atomic.Int32
	handler := func(context.Context, events.Event) error {
		applied.Add(1)
		return nil
	}
	dedup := events.NewMemoryDedupStore()

	beforeCrash := &events.Consumer{Handler: handler, Dedup: dedup}
	if err := beforeCrash.Process(ctx, testEvent()); err != nil {
		t.Fatalf("delivery before crash: %v", err)
	}

	// Crash: beforeCrash is dropped. Restart: a new consumer, same dedup.
	afterRestart := &events.Consumer{Handler: handler, Dedup: dedup}
	if err := afterRestart.Process(ctx, testEvent()); err != nil {
		t.Fatalf("redelivery after restart should be skipped, got error: %v", err)
	}
	if applied.Load() != 1 {
		t.Errorf("handler applied %d times across crash/restart; want 1", applied.Load())
	}

	fresh := testEvent()
	fresh.ID = "tx-2:cash"
	fresh.TransactionID = "tx-2"
	if err := afterRestart.Process(ctx, fresh); err != nil {
		t.Fatalf("new event after restart: %v", err)
	}
	if applied.Load() != 2 {
		t.Errorf("handler applied %d times after a new event; want 2", applied.Load())
	}
}

// TestRelayCrashRestartDrainsInOrderWithoutDuplicates kills the relay
// mid-batch (the broker fails on the second event), then restarts it with
// a healthy publisher. The restarted relay must publish the remaining
// events in outbox order, each exactly once, leaving nothing pending.
func TestRelayCrashRestartDrainsInOrderWithoutDuplicates(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryStore()
	svc := ledger.NewService(mem)
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account %s: %v", acct.ID, err)
		}
	}
	postFixture(t, svc, "sale-1", "first sale", []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 5000},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 5000},
	})
	postFixture(t, svc, "sale-2", "second sale", []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 7000},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 7000},
	})
	pending, err := mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending outbox: %v", err)
	}
	if len(pending) != 4 {
		t.Fatalf("pending outbox = %d records; want 4 (2 accounts x 2 sales)", len(pending))
	}
	wantOrder := make([]string, 0, len(pending))
	for _, record := range pending {
		wantOrder = append(wantOrder, record.Event.ID)
	}

	// Crash mid-batch: the broker fails on the second event. Head-of-line
	// blocking means only the first event is published before the error.
	pub := &recordingPublisher{failOn: map[string]bool{wantOrder[1]: true}}
	relay := &events.Relay{Store: mem, Publisher: pub}
	if _, err := relay.RunOnce(ctx); err == nil {
		t.Fatal("expected relay to fail on the broken broker")
	}
	if got := pub.ids(); len(got) != 1 || got[0] != wantOrder[0] {
		t.Errorf("published before crash = %v; want [%s]", got, wantOrder[0])
	}

	// Restart with a healthy broker: the remaining events drain in the
	// original outbox order, with no duplicates of the first event.
	pub.failOn = map[string]bool{}
	restarted := &events.Relay{Store: mem, Publisher: pub}
	if _, err := restarted.RunOnce(ctx); err != nil {
		t.Fatalf("restarted relay: %v", err)
	}
	if got := pub.ids(); len(got) != len(wantOrder) {
		t.Fatalf("published total = %v; want %v", got, wantOrder)
	} else {
		for i, id := range wantOrder {
			if got[i] != id {
				t.Errorf("published[%d] = %s; want %s (full order %v)", i, got[i], id, wantOrder)
			}
		}
	}
	remaining, err := mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending outbox after restart: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("pending outbox after restart = %d records; want 0", len(remaining))
	}
}

// TestHandlerCrashBeforeMarkIsRedelivered documents the one hole in the
// at-least-once contract honestly: if the consumer crashes after applying
// an event but before marking it processed, the redelivery WILL call the
// handler again. That is why handlers must themselves be idempotent —
// the dedup store can only suppress redeliveries it managed to record.
func TestHandlerCrashBeforeMarkIsRedelivered(t *testing.T) {
	ctx := context.Background()
	var applied atomic.Int32
	dedup := &flakyMarkDedup{inner: events.NewMemoryDedupStore(), failMarkOnce: true}
	consumer := &events.Consumer{
		Handler: func(context.Context, events.Event) error {
			applied.Add(1)
			return nil
		},
		Dedup: dedup,
	}
	event := testEvent()
	if err := consumer.Process(ctx, event); err == nil {
		t.Fatal("expected an error when marking processed fails")
	}
	// Restart with the dedup store healthy: the event is redelivered and
	// applied a second time, then a third delivery is finally suppressed.
	dedup.failMarkOnce = false
	if err := consumer.Process(ctx, event); err != nil {
		t.Fatalf("redelivery after restart: %v", err)
	}
	if err := consumer.Process(ctx, event); err != nil {
		t.Fatalf("third delivery should be skipped, got error: %v", err)
	}
	if applied.Load() != 2 {
		t.Errorf("handler applied %d times; want 2 (once before the crash, once on redelivery)", applied.Load())
	}
}

// flakyMarkDedup fails its first MarkProcessed call, simulating a crash
// between applying an event and recording that fact.
type flakyMarkDedup struct {
	inner        *events.MemoryDedupStore
	failMarkOnce bool
}

func (d *flakyMarkDedup) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	return d.inner.IsProcessed(ctx, eventID)
}

func (d *flakyMarkDedup) MarkProcessed(ctx context.Context, eventID string) error {
	if d.failMarkOnce {
		return errors.New("simulated crash before mark")
	}
	return d.inner.MarkProcessed(ctx, eventID)
}

func (d *flakyMarkDedup) RecordDeadLetter(ctx context.Context, dl events.DeadLetter) error {
	return d.inner.RecordDeadLetter(ctx, dl)
}

func (d *flakyMarkDedup) DeadLetters(ctx context.Context) ([]events.DeadLetter, error) {
	return d.inner.DeadLetters(ctx)
}

func (d *flakyMarkDedup) RemoveDeadLetter(ctx context.Context, eventID string) error {
	return d.inner.RemoveDeadLetter(ctx, eventID)
}
