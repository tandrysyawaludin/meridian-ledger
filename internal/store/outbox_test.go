package store_test

import (
	"context"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// TestMemoryStoreOutbox covers the outbox half of the Store contract
// against the in-memory backend: fan-out, ordering, and the relay's
// mark-published / mark-attempt bookkeeping.
func TestMemoryStoreOutbox(t *testing.T) {
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
	if _, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "sale-1",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 100},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 100},
		},
	}); err != nil {
		t.Fatalf("post: %v", err)
	}

	pending, err := mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want 2 (one per account leg)", len(pending))
	}
	if pending[0].Seq >= pending[1].Seq {
		t.Fatalf("pending not in sequence order: %d, %d", pending[0].Seq, pending[1].Seq)
	}
	if pending[0].Event.AccountID != "cash" || pending[1].Event.AccountID != "revenue" {
		t.Fatalf("pending legs = %q, %q, want cash then revenue",
			pending[0].Event.AccountID, pending[1].Event.AccountID)
	}

	// A failed attempt leaves the record pending; a publish removes it.
	if err := mem.MarkOutboxAttempt(ctx, pending[0].Seq); err != nil {
		t.Fatalf("mark attempt: %v", err)
	}
	if pending, err := mem.PendingOutbox(ctx, 100); err != nil || len(pending) != 2 {
		t.Fatalf("pending after attempt = %d, err=%v, want still 2", len(pending), err)
	}
	if err := mem.MarkOutboxPublished(ctx, pending[0].Seq); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	pending, err = mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 || pending[0].Event.AccountID != "revenue" {
		t.Fatalf("pending after publish = %+v, want only the revenue leg", pending)
	}

	// Limit is honoured.
	if _, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "sale-2",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 50},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 50},
		},
	}); err != nil {
		t.Fatalf("second post: %v", err)
	}
	pending, err = mem.PendingOutbox(ctx, 1)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending with limit 1 = %d, err=%v, want 1", len(pending), err)
	}
}
