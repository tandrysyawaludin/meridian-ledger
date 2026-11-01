package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

func TestMemoryStoreAccountLifecycle(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()

	acct := ledger.Account{ID: "cash", Name: "Cash", Currency: "USD"}
	if err := s.CreateAccount(ctx, acct); err != nil {
		t.Fatalf("create account: %v", err)
	}
	if err := s.CreateAccount(ctx, acct); !errors.Is(err, ledger.ErrAccountExists) {
		t.Fatalf("duplicate create err = %v, want ErrAccountExists", err)
	}
	got, err := s.Account(ctx, "cash")
	if err != nil {
		t.Fatalf("get account: %v", err)
	}
	if got != acct {
		t.Errorf("account = %+v, want %+v", got, acct)
	}
	if _, err := s.Account(ctx, "missing"); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("missing account err = %v, want ErrAccountNotFound", err)
	}
}

func TestMemoryStoreJournalIsAppendOnlyAndCopied(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()

	entries := []ledger.Entry{
		{AccountID: "cash", Direction: ledger.Debit, Amount: 100},
		{AccountID: "revenue", Direction: ledger.Credit, Amount: 100},
	}
	tx := ledger.Transaction{ID: "tx-1", IdempotencyKey: "k-1", Entries: entries}
	if err := s.SaveTransaction(ctx, tx); err != nil {
		t.Fatalf("save transaction: %v", err)
	}

	// Mutating the caller's slice afterwards must not rewrite the journal.
	entries[0].Amount = 999999

	stored, found, err := s.TransactionByIdempotencyKey(ctx, "k-1")
	if err != nil || !found {
		t.Fatalf("lookup by key: found=%v err=%v", found, err)
	}
	if stored.Entries[0].Amount != 100 {
		t.Errorf("stored amount = %d, want 100 (journal must be immutable)", stored.Entries[0].Amount)
	}
	if _, found, err := s.TransactionByIdempotencyKey(ctx, "nope"); err != nil || found {
		t.Errorf("unknown key: found=%v err=%v, want found=false err=nil", found, err)
	}
}

func TestPostgresStoreIsAStubForNow(t *testing.T) {
	ctx := context.Background()
	s := store.NewPostgresStore("postgres://localhost:5432/meridian")

	if err := s.CreateAccount(ctx, ledger.Account{ID: "x"}); !errors.Is(err, store.ErrNotImplemented) {
		t.Fatalf("err = %v, want ErrNotImplemented until the Day 2 pgx implementation lands", err)
	}
}
