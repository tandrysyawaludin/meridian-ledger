// Package events carries ledger postings to downstream systems through a
// transactional outbox.
//
// A posting is fanned out into one event per distinct account it touches.
// That is what makes the ordering guarantee achievable on a partitioned
// topic: an event's partition key is its account, so every event for one
// account lands in one partition, in outbox-sequence order. A single event
// per transaction could not promise that, because one transaction touches
// several accounts and therefore has no single partition key.
//
// Delivery is at-least-once: the relay republishes until the broker
// acknowledges, and the consumer deduplicates on the event ID, so a
// redelivery never applies twice. See docs/EVENTS.md for the full contract.
package events

import (
	"fmt"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

// EventTypeTransactionPosted is the only event type in Day 3. The ".v1"
// suffix is the schema version: a future incompatible payload shape gets a
// new suffix, and consumers can route on it, instead of silently decoding
// a shape they do not understand.
const EventTypeTransactionPosted = "ledger.transaction.posted.v1"

// Event is one account leg of a posted transaction, as published to the
// stream. It carries the full transaction so a consumer does not need a
// second read to act on it, and the leg's account so a per-account
// projection can filter without scanning entries.
type Event struct {
	ID            string       `json:"event_id"`
	Type          string       `json:"type"`
	TransactionID string       `json:"transaction_id"`
	AccountID     string       `json:"account_id"`
	Memo          string       `json:"memo"`
	Entries       []EventEntry `json:"entries"`
	CreatedAt     time.Time    `json:"created_at"`
}

// EventEntry is one leg of the transaction, in the event's wire shape.
// Amounts are integer minor units, as everywhere else in the system.
type EventEntry struct {
	AccountID   string `json:"account_id"`
	Direction   string `json:"direction"`
	AmountMinor int64  `json:"amount_minor"`
}

// PartitionKey returns the Kafka partition key for this event: the leg's
// account. All events for one account share a key and therefore, with a
// key-hashing balancer, a partition, which is what preserves per-account
// order on the broker.
func (e Event) PartitionKey() string { return e.AccountID }

// NewTransactionPosted builds the event for one account leg of tx. It
// returns an error if tx does not touch accountID, so a store can never
// publish an event for an account a transaction did not move.
func NewTransactionPosted(tx ledger.Transaction, accountID string) (Event, error) {
	if tx.ID == "" {
		return Event{}, fmt.Errorf("events: transaction id is required")
	}
	touched := false
	entries := make([]EventEntry, 0, len(tx.Entries))
	for _, e := range tx.Entries {
		if e.AccountID == accountID {
			touched = true
		}
		entries = append(entries, EventEntry{
			AccountID:   e.AccountID,
			Direction:   string(e.Direction),
			AmountMinor: e.Amount,
		})
	}
	if !touched {
		return Event{}, fmt.Errorf("events: transaction %s does not touch account %s", tx.ID, accountID)
	}
	return Event{
		ID:            tx.ID + ":" + accountID,
		Type:          EventTypeTransactionPosted,
		TransactionID: tx.ID,
		AccountID:     accountID,
		Memo:          tx.Memo,
		Entries:       entries,
		CreatedAt:     tx.CreatedAt,
	}, nil
}
