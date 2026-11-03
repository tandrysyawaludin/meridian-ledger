// Package store provides persistence implementations for the ledger domain.
//
// The Store interface is the contract every backend must satisfy. It ships
// with a fully working in-memory implementation (used by tests and local
// development) and a pgx-backed Postgres implementation (used in
// production) that enforces the same contract with database constraints:
// a UNIQUE idempotency key, foreign keys to accounts, and a per-account
// version counter for optimistic-concurrency balance snapshots.
package store

import (
	"context"

	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

// Store is the persistence contract for Meridian Ledger. It is intentionally
// identical in shape to ledger.Repository: the domain defines the port, and
// this package provides the adapters.
type Store interface {
	CreateAccount(ctx context.Context, account ledger.Account) error
	Account(ctx context.Context, id string) (ledger.Account, error)
	SaveTransaction(ctx context.Context, tx ledger.Transaction) error
	Transaction(ctx context.Context, id string) (ledger.Transaction, error)
	TransactionByIdempotencyKey(ctx context.Context, key string) (tx ledger.Transaction, found bool, err error)
	TransactionsForAccount(ctx context.Context, accountID string) ([]ledger.Transaction, error)
	BalanceSnapshot(ctx context.Context, accountID string) (ledger.BalanceSnapshot, error)
	// Outbox methods: the transactional outbox (Day 3). SaveTransaction
	// appends one outbox record per distinct account touched, atomically
	// with the journal write, so an event can never exist without its
	// transaction or vice versa.
	PendingOutbox(ctx context.Context, limit int) ([]events.OutboxRecord, error)
	MarkOutboxPublished(ctx context.Context, seq int64) error
	MarkOutboxAttempt(ctx context.Context, seq int64) error
}

// Compile-time checks that both backends satisfy the contract.
var (
	_ Store = (*MemoryStore)(nil)
	_ Store = (*PostgresStore)(nil)
)
