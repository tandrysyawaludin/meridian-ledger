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

func TestMemoryStoreTransactionLookupAndSnapshotVersions(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	svc := ledger.NewService(s)

	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account %s: %v", acct.ID, err)
		}
	}

	// Version starts at 0 and the balance is empty before any posting.
	snap, err := s.BalanceSnapshot(ctx, "cash")
	if err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	if snap.Balance != 0 || snap.Version != 0 {
		t.Fatalf("initial snapshot = %+v, want balance 0 version 0", snap)
	}

	tx, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "sale-1",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 5000},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 5000},
		},
	})
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	// One posting bumps each touched account's version exactly once.
	snap, err = s.BalanceSnapshot(ctx, "cash")
	if err != nil {
		t.Fatalf("snapshot after posting: %v", err)
	}
	if snap.Balance != 5000 || snap.Version != 1 {
		t.Fatalf("snapshot = %+v, want balance 5000 version 1", snap)
	}

	// Transaction lookup by ID, and the not-found error.
	got, err := s.Transaction(ctx, tx.ID)
	if err != nil || got.IdempotencyKey != "sale-1" {
		t.Fatalf("lookup by id: tx=%+v err=%v", got, err)
	}
	if _, err := s.Transaction(ctx, "missing"); !errors.Is(err, ledger.ErrTransactionNotFound) {
		t.Fatalf("missing tx err = %v, want ErrTransactionNotFound", err)
	}
	if _, err := s.BalanceSnapshot(ctx, "missing"); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("missing account snapshot err = %v, want ErrAccountNotFound", err)
	}
}
