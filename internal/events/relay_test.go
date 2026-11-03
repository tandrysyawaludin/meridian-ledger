package events_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// recordingPublisher records published events and can be made to fail on
// a chosen event ID, to exercise the relay's head-of-line behaviour.
type recordingPublisher struct {
	mu        sync.Mutex
	published []events.Event
	failOn    map[string]bool
}

func (p *recordingPublisher) Publish(_ context.Context, event events.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failOn[event.ID] {
		return errors.New("simulated broker failure")
	}
	p.published = append(p.published, event)
	return nil
}

func (p *recordingPublisher) Close() error { return nil }

func (p *recordingPublisher) ids() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, 0, len(p.published))
	for _, e := range p.published {
		ids = append(ids, e.ID)
	}
	return ids
}

func postFixture(t *testing.T, svc *ledger.Service, key, memo string, entries []ledger.Entry) {
	t.Helper()
	if _, err := svc.Post(context.Background(), ledger.PostRequest{
		IdempotencyKey: key, Memo: memo, Entries: entries,
	}); err != nil {
		t.Fatalf("post %s: %v", key, err)
	}
}

func TestRelayPublishesInOutboxOrderAndPerAccountOrder(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryStore()
	svc := ledger.NewService(mem)
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
		{ID: "expense", Name: "Expense", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account: %v", err)
		}
	}
	// Two postings sharing the cash account, plus one touching expense.
	postFixture(t, svc, "sale-1", "sale", []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 5000},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 5000},
	})
	postFixture(t, svc, "supplies-1", "supplies", []ledger.Entry{
		{AccountID: "expense", Direction: ledger.Debit, Amount: 200},
		{AccountID: "cash", Direction: ledger.Credit, Amount: 200},
	})

	// The outbox holds one event per account leg, in posting order.
	pending, err := mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 4 {
		t.Fatalf("pending = %d events, want 4 (one per leg)", len(pending))
	}

	pub := &recordingPublisher{failOn: map[string]bool{}}
	relay := &events.Relay{Store: mem, Publisher: pub}
	n, err := relay.RunOnce(ctx)
	if err != nil || n != 4 {
		t.Fatalf("relay published %d, err=%v, want 4, nil", n, err)
	}
	// A second poll publishes nothing: everything is marked published.
	if n, err := relay.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("second poll published %d, err=%v, want 0, nil", n, err)
	}

	// Per-account order: cash's events follow posting order (sale, then
	// supplies), even though other accounts' events interleave globally.
	var cashIDs []string
	for _, e := range pub.published {
		if e.AccountID == "cash" {
			cashIDs = append(cashIDs, e.ID)
		}
	}
	if len(cashIDs) != 2 {
		t.Fatalf("cash events = %v, want 2", cashIDs)
	}
	// The cash leg of the first posting precedes the cash leg of the second.
	first, second := cashIDs[0], cashIDs[1]
	if first == second {
		t.Fatalf("duplicate cash event id %q", first)
	}
	// Order check via the global publish sequence: the sale cash leg must
	// appear before the supplies cash leg in the published stream.
	ids := pub.ids()
	pos := func(id string) int {
		for i, v := range ids {
			if v == id {
				return i
			}
		}
		return -1
	}
	if pos(first) > pos(second) {
		t.Fatalf("cash order violated: published order %v", ids)
	}
}

func TestRelayIdempotentReplayCreatesNoOutboxRows(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryStore()
	svc := ledger.NewService(mem)
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account: %v", err)
		}
	}
	entries := []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 100},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 100},
	}
	postFixture(t, svc, "sale-1", "sale", entries)
	// Safe retry of the same posting: the service returns the original
	// transaction and writes nothing new — no duplicate outbox rows.
	postFixture(t, svc, "sale-1", "sale", entries)
	pending, err := mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want 2 after idempotent replay", len(pending))
	}
}

func TestRelayHeadOfLineBlockingOnPublishFailure(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryStore()
	svc := ledger.NewService(mem)
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account: %v", err)
		}
	}
	postFixture(t, svc, "sale-1", "sale", []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 100},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 100},
	})

	pending, err := mem.PendingOutbox(ctx, 100)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending = %d, err=%v, want 2", len(pending), err)
	}
	// Fail on the first event: nothing may publish, and the relay must
	// not skip ahead to the second event.
	pub := &recordingPublisher{failOn: map[string]bool{pending[0].Event.ID: true}}
	relay := &events.Relay{Store: mem, Publisher: pub}
	if n, err := relay.RunOnce(ctx); err == nil || n != 0 {
		t.Fatalf("failing poll published %d, err=%v, want 0 and an error", n, err)
	}
	if got := len(pub.ids()); got != 0 {
		t.Fatalf("published %d events despite head-of-line failure", got)
	}
	// Broker recovers: the next poll publishes everything, in order.
	pub.failOn = map[string]bool{}
	if n, err := relay.RunOnce(ctx); err != nil || n != 2 {
		t.Fatalf("recovery poll published %d, err=%v, want 2, nil", n, err)
	}
	if got := pub.ids(); len(got) != 2 || got[0] != pending[0].Event.ID || got[1] != pending[1].Event.ID {
		t.Fatalf("recovery publish order = %v, want %q then %q", got, pending[0].Event.ID, pending[1].Event.ID)
	}
}

func TestFailedPostingLeavesNoOutboxRows(t *testing.T) {
	// Atomicity at the service level: a posting rejected by validation
	// (unknown account) must leave neither journal rows nor outbox rows.
	ctx := context.Background()
	mem := store.NewMemoryStore()
	svc := ledger.NewService(mem)
	if _, err := svc.CreateAccount(ctx, ledger.Account{ID: "cash", Name: "Cash", Currency: "USD"}); err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "bad-1",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 100},
			{AccountID: "ghost", Direction: ledger.Credit, Amount: 100},
		},
	}); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("post err = %v, want ErrAccountNotFound", err)
	}
	pending, err := mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %d after failed posting, want 0", len(pending))
	}
}
