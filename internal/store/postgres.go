package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

//go:embed schema.sql
var schemaSQL string

// PostgresStore is the production persistence backend, backed by a pgx
// connection pool.
//
// Posting is atomic by construction: the transaction row, its entries, and
// the per-account version bumps all commit in a single database
// transaction, so a crash mid-posting leaves either the whole posting or
// none of it. The UNIQUE constraint on transactions.idempotency_key is the
// cross-process backstop for the domain's idempotency rule (the service
// layer additionally serialises postings within one process).
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore opens a connection pool for dsn (a Postgres connection
// string, e.g. from DATABASE_URL) and applies the schema. The caller must
// call Close when the store is no longer needed. The connection string
// itself is never logged.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

// Close releases the connection pool.
func (p *PostgresStore) Close() { p.pool.Close() }

// TruncateForTest empties every ledger table. It exists only for the
// integration test, which runs against a scratch database designated by
// MERIDIAN_TEST_DATABASE_URL; it must never be called against a database
// holding real data.
func (p *PostgresStore) TruncateForTest(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, `TRUNCATE entries, transactions, accounts, outbox_events RESTART IDENTITY CASCADE`)
	return err
}

// CreateAccount registers an account, rejecting duplicate IDs.
func (p *PostgresStore) CreateAccount(ctx context.Context, account ledger.Account) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO accounts (id, name, currency, created_at) VALUES ($1, $2, $3, $4)`,
		account.ID, account.Name, account.Currency, account.CreatedAt)
	if isUniqueViolation(err) {
		return ledger.ErrAccountExists
	}
	return err
}

// Account returns the account with the given ID.
func (p *PostgresStore) Account(ctx context.Context, id string) (ledger.Account, error) {
	var account ledger.Account
	err := p.pool.QueryRow(ctx,
		`SELECT id, name, currency, created_at FROM accounts WHERE id = $1`, id).
		Scan(&account.ID, &account.Name, &account.Currency, &account.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Account{}, ledger.ErrAccountNotFound
	}
	return account, err
}

// SaveTransaction appends a transaction to the journal in a single database
// transaction: the transaction row, every entry, one version bump per
// distinct account touched, and one outbox row per distinct account
// touched. The outbox rows commit atomically with the journal write —
// that is the whole point of the transactional outbox: a crash anywhere
// in this function leaves either the posting and its events, or neither.
// A unique-violation on the idempotency key maps to
// ErrIdempotencyConflict; a foreign-key failure (unknown account) maps to
// ErrAccountNotFound.
func (p *PostgresStore) SaveTransaction(ctx context.Context, tx ledger.Transaction) error {
	dbTx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = dbTx.Rollback(ctx) }()

	if _, err := dbTx.Exec(ctx,
		`INSERT INTO transactions (id, idempotency_key, memo, created_at) VALUES ($1, $2, $3, $4)`,
		tx.ID, tx.IdempotencyKey, tx.Memo, tx.CreatedAt); err != nil {
		if isUniqueViolation(err) {
			return ledger.ErrIdempotencyConflict
		}
		return err
	}
	for i, e := range tx.Entries {
		if _, err := dbTx.Exec(ctx,
			`INSERT INTO entries (transaction_id, account_id, direction, amount_minor, seq)
			 VALUES ($1, $2, $3, $4, $5)`,
			tx.ID, e.AccountID, string(e.Direction), e.Amount, i); err != nil {
			if isForeignKeyViolation(err) {
				return ledger.ErrAccountNotFound
			}
			return err
		}
	}
	seen := make(map[string]bool)
	for _, e := range tx.Entries {
		if seen[e.AccountID] {
			continue
		}
		seen[e.AccountID] = true
		if _, err := dbTx.Exec(ctx,
			`UPDATE accounts SET version = version + 1 WHERE id = $1`, e.AccountID); err != nil {
			return err
		}
	}
	// Outbox fan-out, in the same database transaction: one event per
	// distinct account, in first-touch order. A failure here rolls back
	// the journal write too, so events and transactions never diverge.
	emitted := make(map[string]bool)
	for _, e := range tx.Entries {
		if emitted[e.AccountID] {
			continue
		}
		emitted[e.AccountID] = true
		event, err := events.NewTransactionPosted(tx, e.AccountID)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("store: marshal outbox event %s: %w", event.ID, err)
		}
		if _, err := dbTx.Exec(ctx,
			`INSERT INTO outbox_events (event_id, event_type, transaction_id, account_id, payload, partition_key, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			event.ID, event.Type, event.TransactionID, event.AccountID, payload, event.PartitionKey(), event.CreatedAt); err != nil {
			return err
		}
	}
	return dbTx.Commit(ctx)
}

// PendingOutbox returns unpublished outbox records in sequence order,
// capped at limit. The relay publishes them in exactly this order.
func (p *PostgresStore) PendingOutbox(ctx context.Context, limit int) ([]events.OutboxRecord, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT seq, payload FROM outbox_events
		  WHERE published_at IS NULL ORDER BY seq LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.OutboxRecord
	for rows.Next() {
		var record events.OutboxRecord
		var payload []byte
		if err := rows.Scan(&record.Seq, &payload); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &record.Event); err != nil {
			return nil, fmt.Errorf("store: unmarshal outbox event seq %d: %w", record.Seq, err)
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

// MarkOutboxPublished stamps an outbox record as published so the relay
// never picks it up again.
func (p *PostgresStore) MarkOutboxPublished(ctx context.Context, seq int64) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE outbox_events SET published_at = now() WHERE seq = $1`, seq)
	return err
}

// MarkOutboxAttempt counts one failed publish attempt for seq. The record
// stays unpublished, so the relay retries it on the next poll.
func (p *PostgresStore) MarkOutboxAttempt(ctx context.Context, seq int64) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE outbox_events SET attempts = attempts + 1 WHERE seq = $1`, seq)
	return err
}

// Transaction returns a single journal entry by ID.
func (p *PostgresStore) Transaction(ctx context.Context, id string) (ledger.Transaction, error) {
	return p.transactionWhere(ctx, `t.id = $1`, id)
}

// TransactionByIdempotencyKey looks up a previously posted transaction by
// its idempotency key.
func (p *PostgresStore) TransactionByIdempotencyKey(ctx context.Context, key string) (ledger.Transaction, bool, error) {
	tx, err := p.transactionWhere(ctx, `t.idempotency_key = $1`, key)
	if errors.Is(err, ledger.ErrTransactionNotFound) {
		return ledger.Transaction{}, false, nil
	}
	if err != nil {
		return ledger.Transaction{}, false, err
	}
	return tx, true, nil
}

// TransactionsForAccount returns, in journal order, every transaction that
// touches the given account.
func (p *PostgresStore) TransactionsForAccount(ctx context.Context, accountID string) ([]ledger.Transaction, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT t.id, t.idempotency_key, t.memo, t.created_at
		   FROM transactions t
		   JOIN entries e ON e.transaction_id = t.id
		  WHERE e.account_id = $1
		  GROUP BY t.id, t.idempotency_key, t.memo, t.created_at, t.journal_seq
		  ORDER BY t.journal_seq`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ledger.Transaction
	for rows.Next() {
		var tx ledger.Transaction
		if err := rows.Scan(&tx.ID, &tx.IdempotencyKey, &tx.Memo, &tx.CreatedAt); err != nil {
			return nil, err
		}
		entries, err := p.entriesFor(ctx, tx.ID)
		if err != nil {
			return nil, err
		}
		tx.Entries = entries
		out = append(out, tx)
	}
	return out, rows.Err()
}

// BalanceSnapshot returns the account's derived balance together with its
// optimistic-concurrency version, computed by a single statement so the
// pair is always consistent.
func (p *PostgresStore) BalanceSnapshot(ctx context.Context, accountID string) (ledger.BalanceSnapshot, error) {
	var snap ledger.BalanceSnapshot
	err := p.pool.QueryRow(ctx,
		`SELECT a.version,
		        COALESCE(SUM(CASE WHEN e.direction = 'debit' THEN e.amount_minor ELSE -e.amount_minor END), 0)
		   FROM accounts a
		   LEFT JOIN entries e ON e.account_id = a.id
		  WHERE a.id = $1
		  GROUP BY a.version`, accountID).
		Scan(&snap.Version, &snap.Balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.BalanceSnapshot{}, ledger.ErrAccountNotFound
	}
	if err != nil {
		return ledger.BalanceSnapshot{}, err
	}
	snap.AccountID = accountID
	return snap, nil
}

// transactionWhere loads one transaction (plus its entries, in posting
// order) matching an arbitrary predicate over the transactions table.
func (p *PostgresStore) transactionWhere(ctx context.Context, predicate string, arg string) (ledger.Transaction, error) {
	var tx ledger.Transaction
	err := p.pool.QueryRow(ctx,
		`SELECT id, idempotency_key, memo, created_at FROM transactions t WHERE `+predicate, arg).
		Scan(&tx.ID, &tx.IdempotencyKey, &tx.Memo, &tx.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Transaction{}, ledger.ErrTransactionNotFound
	}
	if err != nil {
		return ledger.Transaction{}, err
	}
	entries, err := p.entriesFor(ctx, tx.ID)
	if err != nil {
		return ledger.Transaction{}, err
	}
	tx.Entries = entries
	return tx, nil
}

func (p *PostgresStore) entriesFor(ctx context.Context, txID string) ([]ledger.Entry, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT account_id, direction, amount_minor FROM entries
		  WHERE transaction_id = $1 ORDER BY seq`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []ledger.Entry
	for rows.Next() {
		var e ledger.Entry
		var direction string
		if err := rows.Scan(&e.AccountID, &direction, &e.Amount); err != nil {
			return nil, err
		}
		e.Direction = ledger.Direction(direction)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
