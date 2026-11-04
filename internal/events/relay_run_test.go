package events_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// TestRelayRunStopsOnCancel verifies the graceful-shutdown contract the
// service depends on: Run returns promptly once its context is cancelled,
// even with a logger configured and a publisher that would fail.
func TestRelayRunStopsOnCancel(t *testing.T) {
	mem := store.NewMemoryStore()
	pub := &recordingPublisher{failOn: map[string]bool{}}
	relay := &events.Relay{
		Store:        mem,
		Publisher:    pub,
		PollInterval: time.Hour, // long poll: only cancellation should wake it
		Logger:       slog.Default(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(ctx)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("relay Run did not return within 2s of context cancellation")
	}
}
