package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// newService returns a ledger service backed by a fresh in-memory store,
// pre-loaded with a USD cash account and a USD revenue account.
func newService(t *testing.T) (*ledger.Service, ledger.Repository) {
	t.Helper()
	repo := store.NewMemoryStore()
	svc := ledger.NewService(repo)
	ctx := context.Background()
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
		{ID: "ap_revenue", Name: "APAC Revenue", Currency: "JPY"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("create account %s: %v", acct.ID, err)
		}
	}
	return svc, repo
}

func salePosting(key string, amount int64) ledger.PostRequest {
	return ledger.PostRequest{
		IdempotencyKey: key,
		Memo:           "card sale",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: amount},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: amount},
		},
	}
}

func TestBalancedPostingSucceeds(t *testing.T) {
	svc, _ := newService(t)
	ctx := context.Background()

	tx, err := svc.Post(ctx, salePosting("sale-1", 1299))
	if err != nil {
		t.Fatalf("post balanced transaction: %v", err)
	}
	if tx.ID == "" {
		t.Error("expected a transaction ID to be assigned")
	}
	if tx.IdempotencyKey != "sale-1" {
		t.Errorf("idempotency key = %q, want %q", tx.IdempotencyKey, "sale-1")
	}
	if len(tx.Entries) != 2 {
		t.Errorf("entries = %d, want 2", len(tx.Entries))
	}

	cash, err := svc.Balance(ctx, "cash")
	if err != nil {
		t.Fatalf("cash balance: %v", err)
	}
	if cash != 1299 {
		t.Errorf("cash balance = %d, want 1299", cash)
	}
	revenue, err := svc.Balance(ctx, "revenue")
	if err != nil {
		t.Fatalf("revenue balance: %v", err)
	}
	if revenue != -1299 {
		t.Errorf("revenue balance = %d, want -1299 (net credit)", revenue)
	}
}

func TestUnbalancedPostingIsRejected(t *testing.T) {
	svc, _ := newService(t)
	ctx := context.Background()

	_, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "bad-1",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 1000},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 999},
		},
	})
	if !errors.Is(err, ledger.ErrUnbalanced) {
		t.Fatalf("err = %v, want ErrUnbalanced", err)
	}

	// Nothing may be persisted for a rejected posting.
	balance, err := svc.Balance(ctx, "cash")
	if err != nil {
		t.Fatalf("cash balance: %v", err)
	}
	if balance != 0 {
		t.Errorf("cash balance after rejected posting = %d, want 0", balance)
	}
}

func TestIdempotentRepostDoesNotDoubleApply(t *testing.T) {
	svc, repo := newService(t)
	ctx := context.Background()

	first, err := svc.Post(ctx, salePosting("sale-42", 5000))
	if err != nil {
		t.Fatalf("first post: %v", err)
	}
	second, err := svc.Post(ctx, salePosting("sale-42", 5000))
	if err != nil {
		t.Fatalf("idempotent repost: %v", err)
	}
	if second.ID != first.ID {
		t.Errorf("repost returned transaction %s, want original %s", second.ID, first.ID)
	}

	txs, err := repo.TransactionsForAccount(ctx, "cash")
	if err != nil {
		t.Fatalf("transactions for cash: %v", err)
	}
	if len(txs) != 1 {
		t.Errorf("journal holds %d transactions for cash, want exactly 1", len(txs))
	}
	balance, err := svc.Balance(ctx, "cash")
	if err != nil {
		t.Fatalf("cash balance: %v", err)
	}
	if balance != 5000 {
		t.Errorf("cash balance = %d, want 5000 (applied once, not twice)", balance)
	}
}

func TestIdempotencyKeyReuseWithDifferentPayloadConflicts(t *testing.T) {
	svc, _ := newService(t)
	ctx := context.Background()

	if _, err := svc.Post(ctx, salePosting("sale-7", 100)); err != nil {
		t.Fatalf("first post: %v", err)
	}
	_, err := svc.Post(ctx, salePosting("sale-7", 200))
	if !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestBalanceAcrossMultiplePostings(t *testing.T) {
	svc, _ := newService(t)
	ctx := context.Background()

	// Three sales in, one refund out (a reversing posting that debits
	// revenue and credits cash).
	postings := []ledger.PostRequest{
		salePosting("s-1", 1000),
		salePosting("s-2", 2500),
		salePosting("s-3", 750),
		{
			IdempotencyKey: "refund-1",
			Memo:           "refund of s-2 (partial)",
			Entries: []ledger.Entry{
				{AccountID: "revenue", Direction: ledger.Debit, Amount: 500},
				{AccountID: "cash", Direction: ledger.Credit, Amount: 500},
			},
		},
	}
	for _, p := range postings {
		if _, err := svc.Post(ctx, p); err != nil {
			t.Fatalf("post %s: %v", p.IdempotencyKey, err)
		}
	}

	cash, err := svc.Balance(ctx, "cash")
	if err != nil {
		t.Fatalf("cash balance: %v", err)
	}
	if want := int64(1000 + 2500 + 750 - 500); cash != want {
		t.Errorf("cash balance = %d, want %d", cash, want)
	}
	revenue, err := svc.Balance(ctx, "revenue")
	if err != nil {
		t.Fatalf("revenue balance: %v", err)
	}
	if want := int64(-(1000 + 2500 + 750) + 500); revenue != want {
		t.Errorf("revenue balance = %d, want %d", revenue, want)
	}

	// The books as a whole must always net to zero.
	if cash+revenue != 0 {
		t.Errorf("trial balance = %d, want 0", cash+revenue)
	}
}

func TestPostingValidation(t *testing.T) {
	svc, _ := newService(t)
	ctx := context.Background()

	cases := []struct {
		name string
		req  ledger.PostRequest
		want error
	}{
		{
			name: "missing idempotency key",
			req:  ledger.PostRequest{Entries: salePosting("x", 1).Entries},
			want: ledger.ErrEmptyIdempotencyKey,
		},
		{
			name: "single entry",
			req: ledger.PostRequest{
				IdempotencyKey: "one-leg",
				Entries:        []ledger.Entry{{AccountID: "cash", Direction: ledger.Debit, Amount: 10}},
			},
			want: ledger.ErrTooFewEntries,
		},
		{
			name: "zero amount",
			req: ledger.PostRequest{
				IdempotencyKey: "zero",
				Entries: []ledger.Entry{
					{AccountID: "cash", Direction: ledger.Debit, Amount: 0},
					{AccountID: "revenue", Direction: ledger.Credit, Amount: 0},
				},
			},
			want: ledger.ErrInvalidAmount,
		},
		{
			name: "negative amount",
			req: ledger.PostRequest{
				IdempotencyKey: "neg",
				Entries: []ledger.Entry{
					{AccountID: "cash", Direction: ledger.Debit, Amount: -5},
					{AccountID: "revenue", Direction: ledger.Credit, Amount: -5},
				},
			},
			want: ledger.ErrInvalidAmount,
		},
		{
			name: "unknown account",
			req: ledger.PostRequest{
				IdempotencyKey: "ghost",
				Entries: []ledger.Entry{
					{AccountID: "cash", Direction: ledger.Debit, Amount: 10},
					{AccountID: "nowhere", Direction: ledger.Credit, Amount: 10},
				},
			},
			want: ledger.ErrAccountNotFound,
		},
		{
			name: "mixed currencies",
			req: ledger.PostRequest{
				IdempotencyKey: "fx",
				Entries: []ledger.Entry{
					{AccountID: "cash", Direction: ledger.Debit, Amount: 10},
					{AccountID: "ap_revenue", Direction: ledger.Credit, Amount: 10},
				},
			},
			want: ledger.ErrMixedCurrencies,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Post(ctx, tc.req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestMultiEntryBalancedPosting(t *testing.T) {
	svc, _ := newService(t)
	ctx := context.Background()

	// A sale split across two revenue-recognition legs: one debit, two
	// credits. Only the totals must balance.
	_, err := svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: "split-1",
		Memo:           "sale with fee split",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 10000},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 9700},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 300},
		},
	})
	if err != nil {
		t.Fatalf("post split transaction: %v", err)
	}
	revenue, err := svc.Balance(ctx, "revenue")
	if err != nil {
		t.Fatalf("revenue balance: %v", err)
	}
	if revenue != -10000 {
		t.Errorf("revenue balance = %d, want -10000", revenue)
	}
}
