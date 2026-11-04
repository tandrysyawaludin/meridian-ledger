// Package metrics collects Meridian Ledger's operational metrics and
// exposes them in Prometheus text exposition format.
//
// It is deliberately dependency-free: the exposition format is simple
// enough to emit by hand, and keeping this package on the standard library
// preserves the project's rule that infrastructure dependencies must earn
// their place. A Prometheus server scrapes GET /metrics on the service's
// HTTP port (see cmd/meridian).
//
// Series:
//
//	meridian_postings_total{outcome} — postings by outcome: "success" or
//	  "rejected". Every Post call, over HTTP or gRPC, increments exactly
//	  one of these.
//	meridian_posting_rejections_total{reason} — rejected postings by
//	  reason, from ReasonForError: "unbalanced",
//	  "idempotency_conflict", "not_found", "validation", "internal".
//	  This is the series to alert on: a spike in "unbalanced" usually
//	  means a buggy caller, a spike in "idempotency_conflict" means a
//	  caller reusing keys across different payloads.
//	meridian_posting_duration_seconds — histogram of posting latency,
//	  including rejected postings (a fast rejection is still a latency
//	  data point, and excluding it would hide validation cost).
//	meridian_outbox_pending — gauge of unpublished outbox records, i.e.
//	  the relay's backlog. Grows when the broker is down; see docs/EVENTS.md.
//	meridian_outbox_oldest_age_seconds — gauge of how old the oldest
//	  unpublished outbox record is. Together with the pending count this
//	  distinguishes "brief broker blip" from "relay stuck".
package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

// Histogram buckets for posting latency, in seconds. The sub-10ms buckets
// matter: the in-memory path posts in well under a millisecond, so a
// coarse default bucket set would lump every posting into one bucket.
var durationBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
}

// Registry is a goroutine-safe metrics registry. The zero value is not
// usable; use NewRegistry. All methods are safe to call on a nil
// *Registry, where they are no-ops, so callers can hold an optional
// registry without nil checks at every call site.
type Registry struct {
	mu            sync.Mutex
	postings      map[string]uint64 // outcome -> count
	rejections    map[string]uint64 // reason -> count
	durationCount uint64
	durationSum   float64 // seconds
	durationHist  []uint64
	outboxPending int64
	outboxOldest  float64 // seconds; 0 when nothing is pending
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		postings:     make(map[string]uint64),
		rejections:   make(map[string]uint64),
		durationHist: make([]uint64, len(durationBuckets)),
	}
}

// ReasonForError maps a posting error to a stable rejection reason for
// the meridian_posting_rejections_total series. It unwraps wrapped errors,
// so domain errors keep their reason even when wrapped by a transport.
func ReasonForError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ledger.ErrUnbalanced):
		return "unbalanced"
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, ledger.ErrAccountNotFound),
		errors.Is(err, ledger.ErrTransactionNotFound):
		return "not_found"
	case errors.Is(err, ledger.ErrAccountExists):
		return "duplicate_account"
	case errors.Is(err, ledger.ErrEmptyIdempotencyKey),
		errors.Is(err, ledger.ErrTooFewEntries),
		errors.Is(err, ledger.ErrInvalidDirection),
		errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrMixedCurrencies),
		errors.Is(err, ledger.ErrEmptyAccountID),
		errors.Is(err, ledger.ErrInvalidAccount):
		return "validation"
	default:
		return "internal"
	}
}

// ObservePost records one posting attempt: its latency and whether it
// succeeded. On failure it also increments the rejection counter for the
// error's reason.
func (r *Registry) ObservePost(d time.Duration, err error) {
	r.ObservePostOutcome(d, err == nil, ReasonForError(err))
}

// ObservePostOutcome records one posting attempt with an explicit
// outcome. Transports that convert errors before the metrics layer sees
// them (gRPC status codes, which are coarser than domain errors) use
// this to report the reason they can actually observe, instead of losing
// the rejection to "internal".
func (r *Registry) ObservePostOutcome(d time.Duration, ok bool, reason string) {
	if r == nil {
		return
	}
	secs := d.Seconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	if ok {
		r.postings["success"]++
	} else {
		r.postings["rejected"]++
		if reason != "" {
			r.rejections[reason]++
		}
	}
	r.durationCount++
	r.durationSum += secs
	// Count the observation in the first bucket it fits in; the
	// exposition handler renders cumulative buckets from these counts.
	for i, b := range durationBuckets {
		if secs <= b {
			r.durationHist[i]++
			break
		}
	}
}

// SetOutboxLag records the relay backlog: how many outbox records are
// unpublished and how old the oldest one is. Pass pending 0 and age 0
// when the outbox is drained.
func (r *Registry) SetOutboxLag(pending int, oldestAge time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outboxPending = int64(pending)
	r.outboxOldest = oldestAge.Seconds()
}

// Handler returns an http.Handler serving the registry in Prometheus
// text exposition format on any path it is mounted at (conventionally
// GET /metrics).
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if r == nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		out := w
		fmt.Fprintln(out, "# HELP meridian_postings_total Total ledger postings by outcome.")
		fmt.Fprintln(out, "# TYPE meridian_postings_total counter")
		for _, outcome := range sortedKeys(r.postings) {
			fmt.Fprintf(out, "meridian_postings_total{outcome=%q} %d\n", outcome, r.postings[outcome])
		}
		fmt.Fprintln(out, "# HELP meridian_posting_rejections_total Rejected ledger postings by reason.")
		fmt.Fprintln(out, "# TYPE meridian_posting_rejections_total counter")
		for _, reason := range sortedKeys(r.rejections) {
			fmt.Fprintf(out, "meridian_posting_rejections_total{reason=%q} %d\n", reason, r.rejections[reason])
		}
		fmt.Fprintln(out, "# HELP meridian_posting_duration_seconds Posting latency in seconds.")
		fmt.Fprintln(out, "# TYPE meridian_posting_duration_seconds histogram")
		var cumulative uint64
		for i, b := range durationBuckets {
			cumulative += r.durationHist[i]
			fmt.Fprintf(out, "meridian_posting_duration_seconds_bucket{le=%g} %d\n", b, cumulative)
		}
		fmt.Fprintf(out, "meridian_posting_duration_seconds_bucket{le=\"+Inf\"} %d\n", r.durationCount)
		fmt.Fprintf(out, "meridian_posting_duration_seconds_sum %g\n", r.durationSum)
		fmt.Fprintf(out, "meridian_posting_duration_seconds_count %d\n", r.durationCount)
		fmt.Fprintln(out, "# HELP meridian_outbox_pending Unpublished outbox records (relay backlog).")
		fmt.Fprintln(out, "# TYPE meridian_outbox_pending gauge")
		fmt.Fprintf(out, "meridian_outbox_pending %d\n", r.outboxPending)
		fmt.Fprintln(out, "# HELP meridian_outbox_oldest_age_seconds Age of the oldest unpublished outbox record.")
		fmt.Fprintln(out, "# TYPE meridian_outbox_oldest_age_seconds gauge")
		fmt.Fprintf(out, "meridian_outbox_oldest_age_seconds %g\n", r.outboxOldest)
	})
}

func sortedKeys(m map[string]uint64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
