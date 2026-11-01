// Package store provides persistence implementations for the ledger domain.
//
// The Store interface is the contract every backend must satisfy. Day 1
// ships a fully working in-memory implementation (used by tests and local
// development) and a Postgres type that satisfies the interface but is not
// yet wired to a database; the pgx-backed implementation lands on Day 2 of
// the roadmap.
package store

import (
	"context"
	"errors"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

// Store is the persistence contract for Meridian Ledger. It is intentionally
// identical in shape to ledger.Repository: the domain defines the port, and
// this package provides the adapters.
type Store interface {
	CreateAccount(ctx context.Context, account ledger.Account) error
	Account(ctx context.Context, id string) (ledger.Account, error)
	SaveTransaction(ctx context.Context, tx ledger.Transaction) error
	TransactionByIdempotencyKey(ctx context.Context, key string) (tx ledger.Transaction, found bool, err error)
	TransactionsForAccount(ctx context.Context, accountID string) ([]ledger.Transaction, error)
}

// ErrNotImplemented is returned by store backends whose implementation is
// still pending (see the Postgres stub).
var ErrNotImplemented = errors.New("store: not implemented yet")

// Compile-time checks that both backends satisfy the contract.
var (
	_ Store = (*MemoryStore)(nil)
	_ Store = (*PostgresStore)(nil)
)
