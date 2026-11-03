package events_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
)

func sampleEvent(id, account string) events.Event {
	return events.Event{
		ID:            id,
		Type:          events.EventTypeTransactionPosted,
		TransactionID: "tx-1",
		AccountID:     account,
		Entries: []events.EventEntry{
			{AccountID: "cash", Direction: "debit", AmountMinor: 100},
			{AccountID: "revenue", Direction: "credit", AmountMinor: 100},
		},
		CreatedAt: time.Now().UTC(),
	}
}

// countingHandler records calls and fails a configured number of times
// before succeeding, to exercise retry behaviour.
type countingHandler struct {
	mu        sync.Mutex
	calls     int
	failTimes int
	seen      []string
}

func (h *countingHandler) handle(_ context.Context, event events.Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	h.seen = append(h.seen, event.ID)
	if h.calls <= h.failTimes {
		return errors.New("simulated handler failure")
	}
	return nil
}

func TestConsumerDeduplicatesRedelivery(t *testing.T) {
	ctx := context.Background()
	dedup := events.NewMemoryDedupStore()
	handler := &countingHandler{}
	consumer := &events.Consumer{Handler: handler.handle, Dedup: dedup}
	event := sampleEvent("tx-1:cash", "cash")
	if err := consumer.Process(ctx, event); err != nil {
		t.Fatalf("first process: %v", err)
	}
	// Redelivery of the same event: skipped, handler not called again.
	if err := consumer.Process(ctx, event); err != nil {
		t.Fatalf("redelivery process: %v", err)
	}
	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", handler.calls)
	}
}

func TestConsumerRetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()
	dedup := events.NewMemoryDedupStore()
	handler := &countingHandler{failTimes: 2}
	consumer := &events.Consumer{Handler: handler.handle, Dedup: dedup, MaxAttempts: 3}
	if err := consumer.Process(ctx, sampleEvent("tx-1:cash", "cash")); err != nil {
		t.Fatalf("process: %v", err)
	}
	if handler.calls != 3 {
		t.Fatalf("handler calls = %d, want 3 (two failures, then success)", handler.calls)
	}
	letters, err := dedup.DeadLetters(ctx)
	if err != nil || len(letters) != 0 {
		t.Fatalf("dead letters = %v, err=%v, want none", letters, err)
	}
}

func TestConsumerDeadLettersAfterExhaustedAttempts(t *testing.T) {
	ctx := context.Background()
	dedup := events.NewMemoryDedupStore()
	handler := &countingHandler{failTimes: 100} // never succeeds
	consumer := &events.Consumer{Handler: handler.handle, Dedup: dedup, MaxAttempts: 3}
	event := sampleEvent("tx-1:cash", "cash")
	if err := consumer.Process(ctx, event); err == nil {
		t.Fatal("expected an error for a dead-lettered event")
	}
	if handler.calls != 3 {
		t.Fatalf("handler calls = %d, want exactly MaxAttempts (3)", handler.calls)
	}
	letters, err := dedup.DeadLetters(ctx)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	if len(letters) != 1 || letters[0].Event.ID != event.ID || letters[0].Attempts != 3 {
		t.Fatalf("dead letters = %+v, want one record for %s with 3 attempts", letters, event.ID)
	}
	// Redelivery of a dead-lettered event is skipped: no poison loop.
	if err := consumer.Process(ctx, event); err != nil {
		t.Fatalf("redelivery after dead-letter: %v", err)
	}
	if handler.calls != 3 {
		t.Fatalf("handler calls after redelivery = %d, want still 3", handler.calls)
	}
	// Replay after fixing the handler succeeds and clears the record.
	handler.failTimes = 0
	if err := consumer.Replay(ctx, event.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}
	letters, err = dedup.DeadLetters(ctx)
	if err != nil || len(letters) != 0 {
		t.Fatalf("dead letters after replay = %v, err=%v, want none", letters, err)
	}
	// Replaying a non-dead-lettered event is an error, not a silent no-op.
	if err := consumer.Replay(ctx, "no-such-event"); err == nil {
		t.Fatal("expected error replaying an unknown dead letter")
	}
}

func TestConsumerServePreservesOrderAndSurvivesDeadLetters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dedup := events.NewMemoryDedupStore()
	handler := &countingHandler{failTimes: 100} // every event dead-letters
	consumer := &events.Consumer{Handler: handler.handle, Dedup: dedup, MaxAttempts: 1}
	ch := make(chan events.Event)
	done := make(chan struct{})
	go func() {
		consumer.Serve(ctx, ch)
		close(done)
	}()
	// Three events in order; the first two dead-letter but must not stop
	// the stream, so the third is still attempted.
	ch <- sampleEvent("tx-1:cash", "cash")
	ch <- sampleEvent("tx-2:cash", "cash")
	ch <- sampleEvent("tx-3:revenue", "revenue")
	close(ch)
	<-done
	if got := handler.seen; len(got) != 3 || got[0] != "tx-1:cash" || got[1] != "tx-2:cash" || got[2] != "tx-3:revenue" {
		t.Fatalf("serve order = %v, want tx-1, tx-2, tx-3 in order", got)
	}
	letters, err := dedup.DeadLetters(context.Background())
	if err != nil || len(letters) != 3 {
		t.Fatalf("dead letters = %d, err=%v, want 3", len(letters), err)
	}
}
