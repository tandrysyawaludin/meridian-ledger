package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

func TestMemoryStoreOutboxStats(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryStore()

	// Empty outbox: nothing pending, found false.
	if pending, _, found, err := mem.OutboxStats(ctx); err != nil || pending != 0 || found {
		t.Fatalf("empty stats: pending=%d found=%v err=%v", pending, found, err)
	}

	svc := ledger.NewService(mem)
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("seed %s: %v", acct.ID, err)
		}
	}
	if _, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "stats-1",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 100},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 100},
		},
	}); err != nil {
		t.Fatalf("post: %v", err)
	}

	// One posting across two accounts fans out to two outbox rows.
	pending, oldest, found, err := mem.OutboxStats(ctx)
	if err != nil || !found || pending != 2 {
		t.Fatalf("stats after post: pending=%d found=%v err=%v, want pending=2 found=true", pending, found, err)
	}
	if oldest.IsZero() || oldest.After(time.Now()) {
		t.Errorf("oldest should be a plausible past time, got %v", oldest)
	}

	// Publishing one row drops the count; publishing both drains it.
	records, err := mem.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if err := mem.MarkOutboxPublished(ctx, records[0].Seq); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if pending, _, _, err := mem.OutboxStats(ctx); err != nil || pending != 1 {
		t.Fatalf("after one publish: pending=%d err=%v, want 1", pending, err)
	}
	if err := mem.MarkOutboxPublished(ctx, records[1].Seq); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if pending, _, found, err := mem.OutboxStats(ctx); err != nil || pending != 0 || found {
		t.Fatalf("after drain: pending=%d found=%v err=%v", pending, found, err)
	}
}
