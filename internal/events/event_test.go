package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

func sampleTx() ledger.Transaction {
	return ledger.Transaction{
		ID:             "tx-1",
		IdempotencyKey: "sale-1",
		Memo:           "card sale",
		Entries: []ledger.Entry{
			{AccountID: "cash", Direction: ledger.Debit, Amount: 5000},
			{AccountID: "revenue", Direction: ledger.Credit, Amount: 5000},
		},
		CreatedAt: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC),
	}
}

func TestNewTransactionPostedFanOut(t *testing.T) {
	tx := sampleTx()
	cash, err := events.NewTransactionPosted(tx, "cash")
	if err != nil {
		t.Fatalf("cash leg: %v", err)
	}
	revenue, err := events.NewTransactionPosted(tx, "revenue")
	if err != nil {
		t.Fatalf("revenue leg: %v", err)
	}
	// Distinct, deterministic event IDs, one per leg.
	if cash.ID == revenue.ID {
		t.Fatalf("leg event ids collide: %q", cash.ID)
	}
	if cash.ID != "tx-1:cash" || revenue.ID != "tx-1:revenue" {
		t.Fatalf("event ids = %q, %q", cash.ID, revenue.ID)
	}
	// Partition key is the leg's account.
	if cash.PartitionKey() != "cash" || revenue.PartitionKey() != "revenue" {
		t.Fatalf("partition keys = %q, %q", cash.PartitionKey(), revenue.PartitionKey())
	}
	if cash.Type != events.EventTypeTransactionPosted {
		t.Fatalf("type = %q", cash.Type)
	}
	if len(cash.Entries) != 2 || cash.Entries[0].AmountMinor != 5000 {
		t.Fatalf("entries = %+v", cash.Entries)
	}
	// An account the transaction does not touch gets no event.
	if _, err := events.NewTransactionPosted(tx, "ghost"); err == nil {
		t.Fatal("expected error for untouched account")
	}
}

func TestEventJSONRoundTrip(t *testing.T) {
	event, err := events.NewTransactionPosted(sampleTx(), "cash")
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded events.Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ID != event.ID || decoded.AccountID != "cash" ||
		len(decoded.Entries) != 2 || decoded.Entries[1].Direction != "credit" {
		t.Fatalf("round trip = %+v", decoded)
	}
}
