package store

import (
	"context"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

// PostgresStore is the production persistence backend.
//
// STATUS: stub — Day 1 scope. The type satisfies the Store interface so the
// service wiring, configuration and tests can already target it, but every
// method currently returns ErrNotImplemented. The real implementation lands
// on Day 2 of the roadmap (see ROADMAP.md):
//
//   - pgx connection pool, configured via DATABASE_URL.
//   - Schema: accounts(id PK, name, currency, created_at) and
//     transactions(id PK, idempotency_key UNIQUE, memo, created_at) with
//     entries(transaction_id FK, account_id FK, direction, amount_minor)
//     stored as BIGINT minor units.
//   - Posting runs in a single database transaction; the unique constraint
//     on idempotency_key provides exactly the same check-and-insert
//     semantics the in-memory store provides with its map index.
//   - Optimistic concurrency on account version for balance snapshots.
type PostgresStore struct {
	dsn string
}

// NewPostgresStore creates a PostgresStore for the given connection string.
// It does not open a connection yet (see the Day 2 note above).
func NewPostgresStore(dsn string) *PostgresStore {
	return &PostgresStore{dsn: dsn}
}

// DSN returns the configured connection string (credentials included — never
// log this value).
func (p *PostgresStore) DSN() string { return p.dsn }

func (p *PostgresStore) CreateAccount(_ context.Context, _ ledger.Account) error {
	return ErrNotImplemented
}

func (p *PostgresStore) Account(_ context.Context, _ string) (ledger.Account, error) {
	return ledger.Account{}, ErrNotImplemented
}

func (p *PostgresStore) SaveTransaction(_ context.Context, _ ledger.Transaction) error {
	return ErrNotImplemented
}

func (p *PostgresStore) TransactionByIdempotencyKey(_ context.Context, _ string) (ledger.Transaction, bool, error) {
	return ledger.Transaction{}, false, ErrNotImplemented
}

func (p *PostgresStore) TransactionsForAccount(_ context.Context, _ string) ([]ledger.Transaction, error) {
	return nil, ErrNotImplemented
}
