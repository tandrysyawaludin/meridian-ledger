package metrics_test

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/metrics"
)

func scrape(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

func TestRegistryCountsPostingsAndRejectionsByReason(t *testing.T) {
	reg := metrics.NewRegistry()

	reg.ObservePost(2*time.Millisecond, nil)
	reg.ObservePost(1*time.Millisecond, nil)
	reg.ObservePost(500*time.Microsecond, ledger.ErrUnbalanced)
	reg.ObservePost(500*time.Microsecond, fmt.Errorf("posting failed: %w", ledger.ErrUnbalanced))
	reg.ObservePost(500*time.Microsecond, ledger.ErrIdempotencyConflict)
	reg.ObservePost(500*time.Microsecond, ledger.ErrAccountNotFound)
	reg.ObservePost(500*time.Microsecond, errors.New("boom"))

	body := scrape(t, reg)
	for _, want := range []string{
		`meridian_postings_total{outcome="success"} 2`,
		`meridian_postings_total{outcome="rejected"} 5`,
		`meridian_posting_rejections_total{reason="unbalanced"} 2`,
		`meridian_posting_rejections_total{reason="idempotency_conflict"} 1`,
		`meridian_posting_rejections_total{reason="not_found"} 1`,
		`meridian_posting_rejections_total{reason="internal"} 1`,
		`meridian_posting_duration_seconds_count 7`,
		`meridian_posting_duration_seconds_bucket{le="+Inf"} 7`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q\nfull output:\n%s", want, body)
		}
	}
}

func TestRegistryOutboxGauges(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.SetOutboxLag(42, 90*time.Second)
	body := scrape(t, reg)
	if !strings.Contains(body, "meridian_outbox_pending 42") {
		t.Errorf("missing pending gauge:\n%s", body)
	}
	if !strings.Contains(body, "meridian_outbox_oldest_age_seconds 90") {
		t.Errorf("missing oldest-age gauge:\n%s", body)
	}

	reg.SetOutboxLag(0, 0)
	if body := scrape(t, reg); !strings.Contains(body, "meridian_outbox_pending 0") {
		t.Errorf("drained outbox should report 0 pending:\n%s", body)
	}
}

func TestNilRegistryIsNoop(t *testing.T) {
	var reg *metrics.Registry
	// Must not panic: callers hold an optional registry.
	reg.ObservePost(time.Millisecond, nil)
	reg.SetOutboxLag(1, time.Second)
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("nil registry handler: got status %d", rec.Code)
	}
}

func TestReasonForError(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{nil, ""},
		{ledger.ErrUnbalanced, "unbalanced"},
		{ledger.ErrIdempotencyConflict, "idempotency_conflict"},
		{ledger.ErrAccountNotFound, "not_found"},
		{ledger.ErrAccountExists, "duplicate_account"},
		{ledger.ErrTooFewEntries, "validation"},
		{errors.New("something else"), "internal"},
	}
	for _, c := range cases {
		if got := metrics.ReasonForError(c.err); got != c.want {
			t.Errorf("ReasonForError(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
