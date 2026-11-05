package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// failingRepository wraps a Repository and fails SaveTransaction while
// fail is set, standing in for a database that dies mid-posting. Every
// other method delegates to the wrapped store, so the test observes
// exactly what a caller sees when the write path goes down.
type failingRepository struct {
	ledger.Repository
	fail bool
}

func (f *failingRepository) SaveTransaction(ctx context.Context, tx ledger.Transaction) error {
	if f.fail {
		return errors.New("simulated database failure")
	}
	return f.Repository.SaveTransaction(ctx, tx)
}

// TestKilledDatabaseMidPostingLeavesNoPartialState is the Day 5
// failure-injection test for a database that dies during a posting.
// The posting must fail outright (no half-written journal entry, no
// consumed idempotency key), and the same request must succeed cleanly
// once the database recovers — that is what makes the client retry safe.
// The Postgres backend gets the same property structurally: journal and
// outbox rows commit in one database transaction, so a crash can never
// leave one without the other.
func TestKilledDatabaseMidPostingLeavesNoPartialState(t *testing.T) {
	ctx := context.Background()
	repo := &failingRepository{Repository: store.NewMemoryStore(), fail: true}
	svc := ledger.NewService(repo)
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account %s: %v", acct.ID, err)
		}
	}

	if _, err := svc.Post(ctx, salePosting("sale-1", 5000)); err == nil {
		t.Fatal("expected posting to fail while the database is down")
	}

	// No partial state: balances unchanged, and the idempotency key was
	// not consumed by the failed attempt.
	if bal, err := svc.Balance(ctx, "cash"); err != nil || bal != 0 {
		t.Errorf("balance after failed posting = %d, err = %v; want 0, nil", bal, err)
	}
	if _, found, err := repo.TransactionByIdempotencyKey(ctx, "sale-1"); err != nil || found {
		t.Errorf("idempotency lookup after failed posting: found = %v, err = %v; want not found", found, err)
	}

	// The database recovers; the identical retry must now succeed, and
	// must land exactly once.
	repo.fail = false
	if _, err := svc.Post(ctx, salePosting("sale-1", 5000)); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	if bal, err := svc.Balance(ctx, "cash"); err != nil || bal != 5000 {
		t.Errorf("balance after retry = %d, err = %v; want 5000, nil", bal, err)
	}
	txs, err := repo.TransactionsForAccount(ctx, "cash")
	if err != nil {
		t.Fatalf("transactions for cash: %v", err)
	}
	if len(txs) != 1 {
		t.Errorf("transactions for cash = %d; want exactly 1 after one successful retry", len(txs))
	}
}
