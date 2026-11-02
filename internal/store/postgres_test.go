package store_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// TestPostgresStoreIntegration exercises the pgx backend against a real
// Postgres. It is skipped unless MERIDIAN_TEST_DATABASE_URL points at a
// scratch database: the test truncates the ledger tables on setup, so it
// must never point at a database holding real data.
//
// Run it locally with, for example:
//
//	MERIDIAN_TEST_DATABASE_URL=postgres://postgres@localhost:5432/meridian_test go test ./internal/store/ -run TestPostgresStoreIntegration -v
func TestPostgresStoreIntegration(t *testing.T) {
	dsn := os.Getenv("MERIDIAN_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MERIDIAN_TEST_DATABASE_URL not set; skipping real-Postgres integration test")
	}
	ctx := context.Background()
	pg, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}
	defer pg.Close()

	// Scratch-database setup: start from an empty journal.
	if err := pg.TruncateForTest(ctx); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	svc := ledger.NewService(pg)
	suffix := time.Now().UnixNano()
	cash := "cash-" + itoa(suffix)
	revenue := "revenue-" + itoa(suffix)
	for _, acct := range []ledger.Account{
		{ID: cash, Name: "Cash", Currency: "USD"},
		{ID: revenue, Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account %s: %v", acct.ID, err)
		}
	}
	if _, err := svc.CreateAccount(ctx, ledger.Account{ID: cash, Name: "Cash", Currency: "USD"}); !errors.Is(err, ledger.ErrAccountExists) {
		t.Fatalf("duplicate account err = %v, want ErrAccountExists", err)
	}

	tx, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "sale-1",
		Memo:           "card sale",
		Entries: []ledger.Entry{
			{AccountID: cash, Direction: ledger.Debit, Amount: 5000},
			{AccountID: revenue, Direction: ledger.Credit, Amount: 5000},
		},
	})
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	// Replay returns the original transaction; a conflicting reuse is rejected.
	replay, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "sale-1",
		Memo:           "card sale",
		Entries: []ledger.Entry{
			{AccountID: cash, Direction: ledger.Debit, Amount: 5000},
			{AccountID: revenue, Direction: ledger.Credit, Amount: 5000},
		},
	})
	if err != nil || replay.ID != tx.ID {
		t.Fatalf("replay: tx=%+v err=%v, want id %s", replay, err, tx.ID)
	}
	if _, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "sale-1",
		Entries: []ledger.Entry{
			{AccountID: cash, Direction: ledger.Debit, Amount: 6000},
			{AccountID: revenue, Direction: ledger.Credit, Amount: 6000},
		},
	}); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("conflicting reuse err = %v, want ErrIdempotencyConflict", err)
	}

	// Lookup by ID, journal order, and the versioned balance snapshot.
	got, err := svc.Transaction(ctx, tx.ID)
	if err != nil || got.Memo != "card sale" || len(got.Entries) != 2 {
		t.Fatalf("lookup by id: tx=%+v err=%v", got, err)
	}
	txs, err := pg.TransactionsForAccount(ctx, cash)
	if err != nil || len(txs) != 1 || txs[0].ID != tx.ID {
		t.Fatalf("transactions for account: %v err=%v", txs, err)
	}
	snap, err := svc.BalanceSnapshot(ctx, cash)
	if err != nil {
		t.Fatalf("balance snapshot: %v", err)
	}
	if snap.Balance != 5000 || snap.Version != 1 {
		t.Fatalf("snapshot = %+v, want balance 5000 version 1", snap)
	}

	// A second posting bumps the version again; the balance accumulates.
	if _, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "sale-2",
		Entries: []ledger.Entry{
			{AccountID: cash, Direction: ledger.Debit, Amount: 2500},
			{AccountID: revenue, Direction: ledger.Credit, Amount: 2500},
		},
	}); err != nil {
		t.Fatalf("second post: %v", err)
	}
	snap, err = svc.BalanceSnapshot(ctx, cash)
	if err != nil || snap.Balance != 7500 || snap.Version != 2 {
		t.Fatalf("snapshot after second posting = %+v err=%v, want balance 7500 version 2", snap, err)
	}

	// Posting against an unknown account is rejected, and the failed
	// posting leaves no trace in the journal.
	if _, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "bad-1",
		Entries: []ledger.Entry{
			{AccountID: cash, Direction: ledger.Debit, Amount: 100},
			{AccountID: "ghost", Direction: ledger.Credit, Amount: 100},
		},
	}); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("unknown account post err = %v, want ErrAccountNotFound", err)
	}
	if _, found, err := pg.TransactionByIdempotencyKey(ctx, "bad-1"); err != nil || found {
		t.Fatalf("failed posting persisted: found=%v err=%v", found, err)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
