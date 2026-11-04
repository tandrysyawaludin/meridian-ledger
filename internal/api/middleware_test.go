package api_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/api"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/metrics"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// newLimitedServer builds a handler with a tiny posting rate limit so
// the 429 path is reachable without waiting for a real refill window.
func newLimitedServer(t *testing.T, cfg api.RateLimitConfig) (*httptest.Server, *metrics.Registry) {
	t.Helper()
	svc := ledger.NewService(store.NewMemoryStore())
	ctx := context.Background()
	for _, acct := range []ledger.Account{
		{ID: "cash", Name: "Cash", Currency: "USD"},
		{ID: "revenue", Name: "Revenue", Currency: "USD"},
	} {
		if _, err := svc.CreateAccount(ctx, acct); err != nil {
			t.Fatalf("seed account %s: %v", acct.ID, err)
		}
	}
	reg := metrics.NewRegistry()
	handler := api.NewHandlerWithOptions(svc, api.Options{
		Metrics:   reg,
		Logger:    slog.New(slog.NewTextHandler(testWriter{t}, nil)),
		RateLimit: cfg,
	})
	return httptest.NewServer(handler), reg
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestPostingRateLimitReturns429(t *testing.T) {
	srv, _ := newLimitedServer(t, api.RateLimitConfig{RequestsPerSecond: 1, Burst: 2})
	defer srv.Close()

	post := func(key string) int {
		status, _ := do(t, "POST", srv.URL+"/transactions", `{"idempotency_key":"`+key+`","entries":[
			{"account_id":"cash","direction":"debit","amount_minor":100},
			{"account_id":"revenue","direction":"credit","amount_minor":100}]}`)
		return status
	}
	if s := post("rl-1"); s != http.StatusCreated {
		t.Fatalf("first post: got %d, want 201", s)
	}
	if s := post("rl-2"); s != http.StatusCreated {
		t.Fatalf("second post (burst): got %d, want 201", s)
	}
	// Burst exhausted at 1 req/s; the third post must be limited.
	// httptest clients share one remote IP, so all three hit one bucket.
	status, body := do(t, "POST", srv.URL+"/transactions", `{"idempotency_key":"rl-3","entries":[
		{"account_id":"cash","direction":"debit","amount_minor":100},
		{"account_id":"revenue","direction":"credit","amount_minor":100}]}`)
	if status != http.StatusTooManyRequests || body["code"] != "rate_limited" {
		t.Fatalf("third post: got status %d body %v, want 429 rate_limited", status, body)
	}

	// Reads are not rate-limited: a burst of balance reads must succeed.
	for i := 0; i < 5; i++ {
		if s, _ := do(t, "GET", srv.URL+"/accounts/cash/balance", ""); s != http.StatusOK {
			t.Fatalf("balance read %d: got %d, want 200", i, s)
		}
	}
}

func TestPostingMetricsRecordedOverHTTP(t *testing.T) {
	srv, reg := newLimitedServer(t, api.RateLimitConfig{})
	defer srv.Close()

	// One success and one rejection (unbalanced).
	if s, _ := do(t, "POST", srv.URL+"/transactions", `{"idempotency_key":"m-1","entries":[
		{"account_id":"cash","direction":"debit","amount_minor":100},
		{"account_id":"revenue","direction":"credit","amount_minor":100}]}`); s != http.StatusCreated {
		t.Fatalf("success post: got %d", s)
	}
	if s, _ := do(t, "POST", srv.URL+"/transactions", `{"idempotency_key":"m-2","entries":[
		{"account_id":"cash","direction":"debit","amount_minor":100},
		{"account_id":"revenue","direction":"credit","amount_minor":50}]}`); s != http.StatusBadRequest {
		t.Fatalf("unbalanced post: got %d, want 400", s)
	}

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`meridian_postings_total{outcome="success"} 1`,
		`meridian_postings_total{outcome="rejected"} 1`,
		`meridian_posting_rejections_total{reason="unbalanced"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestRequestIDMiddleware(t *testing.T) {
	srv, _ := newLimitedServer(t, api.RateLimitConfig{})
	defer srv.Close()

	// Caller-supplied ID is echoed.
	req, err := http.NewRequest("GET", srv.URL+"/accounts/cash", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Request-ID", "client-trace-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Request-ID"); got != "client-trace-1" {
		t.Errorf("echoed request id = %q, want client-trace-1", got)
	}

	// Absent ID is generated, not left empty.
	resp2, err := http.Get(srv.URL + "/accounts/cash")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if got := resp2.Header.Get("X-Request-ID"); got == "" {
		t.Error("generated request id missing from response header")
	}
}

func TestRateLimiterRefills(t *testing.T) {
	limiter := api.NewRateLimiter(api.RateLimitConfig{RequestsPerSecond: 100, Burst: 1})
	if limiter == nil {
		t.Fatal("expected a limiter for a positive rate")
	}
	if !limiter.Allow("10.0.0.1") {
		t.Fatal("first request should pass with a full bucket")
	}
	if limiter.Allow("10.0.0.1") {
		t.Fatal("second immediate request should be limited")
	}
	// A different client IP has its own bucket.
	if !limiter.Allow("10.0.0.2") {
		t.Fatal("a different client should not be limited by another's bucket")
	}
	time.Sleep(30 * time.Millisecond)
	if !limiter.Allow("10.0.0.1") {
		t.Error("bucket should have refilled after 30ms at 100 req/s")
	}
}

func TestRateLimiterDisabledOnZeroRate(t *testing.T) {
	if got := api.NewRateLimiter(api.RateLimitConfig{}); got != nil {
		t.Errorf("zero rate should disable the limiter, got %v", got)
	}
}
