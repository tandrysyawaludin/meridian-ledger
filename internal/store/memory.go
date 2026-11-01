package store

import (
	"context"
	"sync"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

// MemoryStore is a goroutine-safe, in-memory Store. It keeps the journal in
// insertion order and indexes transactions by idempotency key, mirroring the
// constraints the Postgres schema enforces with a primary key and a unique
// index. It is the reference implementation for the Store contract and the
// backend used by the unit tests.
type MemoryStore struct {
	mu       sync.RWMutex
	accounts map[string]ledger.Account
	journal  []ledger.Transaction
	byKey    map[string]int // idempotency key -> index into journal
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		accounts: make(map[string]ledger.Account),
		byKey:    make(map[string]int),
	}
}

// CreateAccount registers an account, rejecting duplicate IDs.
func (m *MemoryStore) CreateAccount(_ context.Context, account ledger.Account) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.accounts[account.ID]; exists {
		return ledger.ErrAccountExists
	}
	m.accounts[account.ID] = account
	return nil
}

// Account returns the account with the given ID.
func (m *MemoryStore) Account(_ context.Context, id string) (ledger.Account, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	account, ok := m.accounts[id]
	if !ok {
		return ledger.Account{}, ledger.ErrAccountNotFound
	}
	return account, nil
}

// SaveTransaction appends a transaction to the journal. The journal is
// append-only: entries are deep-copied on the way in so later mutation of
// the caller's slice cannot rewrite history.
func (m *MemoryStore) SaveTransaction(_ context.Context, tx ledger.Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored := tx
	stored.Entries = append([]ledger.Entry(nil), tx.Entries...)
	m.byKey[tx.IdempotencyKey] = len(m.journal)
	m.journal = append(m.journal, stored)
	return nil
}

// TransactionByIdempotencyKey looks up a previously posted transaction by
// its idempotency key.
func (m *MemoryStore) TransactionByIdempotencyKey(_ context.Context, key string) (ledger.Transaction, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx, ok := m.byKey[key]
	if !ok {
		return ledger.Transaction{}, false, nil
	}
	tx := m.journal[idx]
	tx.Entries = append([]ledger.Entry(nil), tx.Entries...)
	return tx, true, nil
}

// TransactionsForAccount returns, in journal order, every transaction that
// touches the given account.
func (m *MemoryStore) TransactionsForAccount(_ context.Context, accountID string) ([]ledger.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []ledger.Transaction
	for _, tx := range m.journal {
		for _, e := range tx.Entries {
			if e.AccountID == accountID {
				cp := tx
				cp.Entries = append([]ledger.Entry(nil), tx.Entries...)
				out = append(out, cp)
				break
			}
		}
	}
	return out, nil
}
