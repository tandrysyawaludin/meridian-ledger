// Package ledger implements the core double-entry bookkeeping domain for
// Meridian Ledger.
//
// The package is deliberately persistence-agnostic: it depends only on the
// Repository port defined here, so the same domain logic runs against the
// in-memory store used in tests and the Postgres store used in production.
//
// Core invariants enforced here, on every posting:
//
//   - Amounts are integer minor units (cents, sen, fils, ...). Floats are
//     never used for money anywhere in the system.
//   - A transaction must contain at least two entries.
//   - Every entry amount must be strictly positive; direction carries the sign.
//   - Total debits must equal total credits, exactly. An unbalanced posting
//     is rejected and nothing is persisted.
//   - All accounts referenced by a transaction must exist and share a single
//     currency. Cross-currency transfers belong in an explicit FX flow, not
//     in a single balanced transaction.
//   - Every posting carries a caller-supplied idempotency key. Re-posting the
//     same key with the same payload returns the original transaction without
//     applying it twice; re-using the key with a different payload is a
//     conflict and is rejected.
package ledger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Direction identifies whether an entry is a debit or a credit.
type Direction string

const (
	Debit  Direction = "debit"
	Credit Direction = "credit"
)

// Account is a single ledger account. Balance is never stored as a mutable
// field on the account in the domain model; it is always derived from the
// account's entries so the journal remains the single source of truth.
type Account struct {
	ID        string
	Name      string
	Currency  string // ISO 4217 code, e.g. "USD", "JPY", "EUR".
	CreatedAt time.Time
}

// Entry is one leg of a transaction: a debit or a credit of Amount minor
// units against AccountID.
type Entry struct {
	AccountID string
	Direction Direction
	Amount    int64 // Minor units, strictly positive.
}

// Transaction is an immutable, balanced set of entries recorded as one
// atomic posting in the journal.
type Transaction struct {
	ID             string
	IdempotencyKey string
	Memo           string
	Entries        []Entry
	CreatedAt      time.Time
}

// PostRequest is the input to Service.Post.
type PostRequest struct {
	IdempotencyKey string
	Memo           string
	Entries        []Entry
}

// Repository is the persistence port the ledger domain depends on.
// Implementations must preserve insertion order for transactions and must
// treat the journal as append-only: transactions are never updated or
// deleted, only superseded by new reversing transactions.
type Repository interface {
	CreateAccount(ctx context.Context, account Account) error
	Account(ctx context.Context, id string) (Account, error)
	SaveTransaction(ctx context.Context, tx Transaction) error
	TransactionByIdempotencyKey(ctx context.Context, key string) (tx Transaction, found bool, err error)
	TransactionsForAccount(ctx context.Context, accountID string) ([]Transaction, error)
}

// Domain errors. Callers should match these with errors.Is.
var (
	ErrAccountExists       = errors.New("ledger: account already exists")
	ErrAccountNotFound     = errors.New("ledger: account not found")
	ErrEmptyIdempotencyKey = errors.New("ledger: idempotency key is required")
	ErrIdempotencyConflict = errors.New("ledger: idempotency key already used with a different payload")
	ErrTooFewEntries       = errors.New("ledger: a transaction requires at least two entries")
	ErrInvalidDirection    = errors.New("ledger: entry direction must be debit or credit")
	ErrInvalidAmount       = errors.New("ledger: entry amount must be a positive number of minor units")
	ErrUnbalanced          = errors.New("ledger: total debits must equal total credits")
	ErrMixedCurrencies     = errors.New("ledger: all accounts in a transaction must share one currency")
	ErrEmptyAccountID      = errors.New("ledger: entry account id is required")
	ErrInvalidAccount      = errors.New("ledger: account id, name and currency are required")
)

// Service is the double-entry ledger. It is safe for concurrent use.
type Service struct {
	repo Repository
	now  func() time.Time

	// mu serialises the idempotency check-and-insert so two concurrent
	// postings with the same key cannot both apply. In the Postgres
	// implementation this role is played by a unique constraint on the
	// idempotency key plus an insert-on-conflict read; the in-memory path
	// keeps the same semantics with this lock.
	mu sync.Mutex
}

// NewService returns a ledger Service backed by repo.
func NewService(repo Repository) *Service {
	return &Service{repo: repo, now: time.Now}
}

// CreateAccount registers a new account. Account IDs are caller-supplied so
// they can mirror an external chart of accounts.
func (s *Service) CreateAccount(ctx context.Context, account Account) (Account, error) {
	if account.ID == "" || account.Name == "" || account.Currency == "" {
		return Account{}, ErrInvalidAccount
	}
	if account.CreatedAt.IsZero() {
		account.CreatedAt = s.now().UTC()
	}
	if err := s.repo.CreateAccount(ctx, account); err != nil {
		return Account{}, err
	}
	return account, nil
}

// Account returns the account with the given ID.
func (s *Service) Account(ctx context.Context, id string) (Account, error) {
	return s.repo.Account(ctx, id)
}

// Post validates and records a balanced transaction.
//
// If a transaction with the same idempotency key already exists, Post
// returns that original transaction and applies nothing again — provided the
// payload is identical. Re-using the key with a different payload returns
// ErrIdempotencyConflict.
func (s *Service) Post(ctx context.Context, req PostRequest) (Transaction, error) {
	if req.IdempotencyKey == "" {
		return Transaction{}, ErrEmptyIdempotencyKey
	}
	if err := validateEntries(req.Entries); err != nil {
		return Transaction{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, found, err := s.repo.TransactionByIdempotencyKey(ctx, req.IdempotencyKey); err != nil {
		return Transaction{}, err
	} else if found {
		if !samePayload(existing, req) {
			return Transaction{}, ErrIdempotencyConflict
		}
		return existing, nil
	}

	if err := s.checkAccounts(ctx, req.Entries); err != nil {
		return Transaction{}, err
	}

	tx := Transaction{
		ID:             newID(),
		IdempotencyKey: req.IdempotencyKey,
		Memo:           req.Memo,
		Entries:        append([]Entry(nil), req.Entries...),
		CreatedAt:      s.now().UTC(),
	}
	if err := s.repo.SaveTransaction(ctx, tx); err != nil {
		return Transaction{}, err
	}
	return tx, nil
}

// Balance derives an account's balance from the journal: total debits minus
// total credits, in minor units. A positive balance is a net debit balance,
// which is the normal state for asset and expense accounts; liability,
// equity and revenue accounts normally carry a net credit (negative) balance
// under this convention.
func (s *Service) Balance(ctx context.Context, accountID string) (int64, error) {
	if _, err := s.repo.Account(ctx, accountID); err != nil {
		return 0, err
	}
	txs, err := s.repo.TransactionsForAccount(ctx, accountID)
	if err != nil {
		return 0, err
	}
	var balance int64
	for _, tx := range txs {
		for _, e := range tx.Entries {
			if e.AccountID != accountID {
				continue
			}
			switch e.Direction {
			case Debit:
				balance += e.Amount
			case Credit:
				balance -= e.Amount
			}
		}
	}
	return balance, nil
}

// validateEntries enforces the structural invariants of a posting: entry
// count, directions, positive amounts, and the balanced-books rule.
func validateEntries(entries []Entry) error {
	if len(entries) < 2 {
		return ErrTooFewEntries
	}
	var debits, credits int64
	for _, e := range entries {
		if e.AccountID == "" {
			return ErrEmptyAccountID
		}
		if e.Amount <= 0 {
			return fmt.Errorf("%w: got %d", ErrInvalidAmount, e.Amount)
		}
		switch e.Direction {
		case Debit:
			debits += e.Amount
		case Credit:
			credits += e.Amount
		default:
			return fmt.Errorf("%w: got %q", ErrInvalidDirection, e.Direction)
		}
	}
	if debits != credits {
		return fmt.Errorf("%w: debits=%d credits=%d", ErrUnbalanced, debits, credits)
	}
	return nil
}

// checkAccounts verifies that every referenced account exists and that all
// of them share a single currency.
func (s *Service) checkAccounts(ctx context.Context, entries []Entry) error {
	currency := ""
	seen := make(map[string]bool)
	for _, e := range entries {
		if seen[e.AccountID] {
			continue
		}
		seen[e.AccountID] = true
		account, err := s.repo.Account(ctx, e.AccountID)
		if err != nil {
			return err
		}
		if currency == "" {
			currency = account.Currency
		} else if account.Currency != currency {
			return fmt.Errorf("%w: %s is %s, expected %s", ErrMixedCurrencies, account.ID, account.Currency, currency)
		}
	}
	return nil
}

// samePayload reports whether an already-stored transaction matches the
// incoming request, comparing entries order-insensitively.
func samePayload(tx Transaction, req PostRequest) bool {
	if tx.Memo != req.Memo || len(tx.Entries) != len(req.Entries) {
		return false
	}
	a := append([]Entry(nil), tx.Entries...)
	b := append([]Entry(nil), req.Entries...)
	sortEntries(a)
	sortEntries(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].AccountID != entries[j].AccountID {
			return entries[i].AccountID < entries[j].AccountID
		}
		if entries[i].Direction != entries[j].Direction {
			return entries[i].Direction < entries[j].Direction
		}
		return entries[i].Amount < entries[j].Amount
	})
}

// newID returns a random 128-bit identifier as hex. Collisions are
// cryptographically negligible; the store layer additionally keys the
// journal by this ID.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is not recoverable in any meaningful way here.
		panic(fmt.Sprintf("ledger: read random bytes: %v", err))
	}
	return hex.EncodeToString(b[:])
}
