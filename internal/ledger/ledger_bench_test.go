package ledger_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

func newBenchService(b *testing.B) *ledger.Service {
	b.Helper()
	svc := ledger.NewService(store.NewMemoryStore())
	ctx := context.Background()
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			b.Fatalf("seed %s: %v", acct.ID, err)
		}
	}
	return svc
}

// BenchmarkPost measures the in-memory posting path end to end: domain
// validation, idempotency check, journal append, and outbox fan-out.
// Each iteration uses a fresh idempotency key so no iteration is a
// replay; the timer excludes setup.
func BenchmarkPost(b *testing.B) {
	svc := newBenchService(b)
	ctx := context.Background()
	entries := []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 1000},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 1000},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.Post(ctx, ledger.PostRequest{
			IdempotencyKey: fmt.Sprintf("bench-%d", i), Entries: entries,
		}); err != nil {
			b.Fatalf("post %d: %v", i, err)
		}
	}
}

// BenchmarkBalanceSnapshot measures the derived-balance read after the
// journal holds 1,000 postings. The balance is a fold over the journal,
// so this benchmark is the honest cost of the derived-balance design —
// and the number to watch if the journal ever grows large enough to
// need a materialized balance.
func BenchmarkBalanceSnapshot(b *testing.B) {
	svc := newBenchService(b)
	ctx := context.Background()
	entries := []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 1000},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 1000},
	}
	for i := 0; i < 1000; i++ {
		if _, err := svc.Post(ctx, ledger.PostRequest{
			IdempotencyKey: fmt.Sprintf("bench-seed-%d", i), Entries: entries,
		}); err != nil {
			b.Fatalf("seed %d: %v", i, err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.BalanceSnapshot(ctx, "cash"); err != nil {
			b.Fatalf("snapshot: %v", err)
		}
	}
}
