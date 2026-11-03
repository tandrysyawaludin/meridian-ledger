-- Meridian Ledger schema (Day 2).
--
-- Design notes:
--   * Amounts are BIGINT minor units; there is no float anywhere.
--   * transactions.idempotency_key is UNIQUE: it is the database-level
--     backstop for the domain's idempotency rule, so two concurrent
--     postings with the same key cannot both land.
--   * journal_seq is a monotonically increasing sequence assigned at insert
--     time. Journal order is journal_seq order, which is stable even when
--     two transactions share a created_at timestamp.
--   * accounts.version is the optimistic-concurrency counter for balance
--     snapshots. It increments once per posting that touches the account,
--     inside the same database transaction as the journal write, so a
--     snapshot's (balance, version) pair can never straddle a posting.

CREATE TABLE IF NOT EXISTS accounts (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    currency   TEXT NOT NULL,
    version    BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    memo            TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL,
    journal_seq     BIGSERIAL UNIQUE
);

CREATE TABLE IF NOT EXISTS entries (
    transaction_id TEXT NOT NULL REFERENCES transactions (id),
    account_id     TEXT NOT NULL REFERENCES accounts (id),
    direction      TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_minor   BIGINT NOT NULL CHECK (amount_minor > 0),
    seq            INT NOT NULL,
    PRIMARY KEY (transaction_id, seq)
);

CREATE INDEX IF NOT EXISTS entries_account_idx ON entries (account_id, transaction_id);

-- Transactional outbox (Day 3).
--
-- One row per published event, written in the same database transaction
-- as the journal posting (see PostgresStore.SaveTransaction), so a crash
-- can never produce a journal entry without its event or an event without
-- its journal entry. The relay publishes rows in seq order and stamps
-- published_at; unpublished rows are exactly the relay's work queue.
-- event_id is UNIQUE so a replayed posting can never double-enqueue an
-- event, and partition_key is the leg's account, which is what the Kafka
-- publisher keys on to preserve per-account order on the broker.
CREATE TABLE IF NOT EXISTS outbox_events (
    seq            BIGSERIAL PRIMARY KEY,
    event_id       TEXT NOT NULL UNIQUE,
    event_type     TEXT NOT NULL,
    transaction_id TEXT NOT NULL REFERENCES transactions (id),
    account_id     TEXT NOT NULL REFERENCES accounts (id),
    payload        JSONB NOT NULL,
    partition_key  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ,
    attempts       INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox_events (seq) WHERE published_at IS NULL;
