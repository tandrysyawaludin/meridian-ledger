package api

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/tracing"
)

// Middleware in this file is the request-scoped observability layer for
// the HTTP API: every request gets a correlation ID, one structured log
// line, and (for postings) a rate-limit check. The chain runs in
// Handler.ServeHTTP, outside the route mux, so it covers every endpoint
// including future ones.

type ctxKey string

const requestIDKey ctxKey = "request-id"

// RequestIDFromContext returns the correlation ID assigned to the request
// by the request-ID middleware, or "" when the request did not pass
// through it (direct handler tests, for example).
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// requestIDMiddleware assigns a correlation ID: it honors a caller-supplied
// X-Request-ID so a client can trace its own request end to end, and
// generates one otherwise. The ID is echoed in the response header and
// stored in the request context, where the logging middleware and any
// future handler code read it from.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = tracing.NewID()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

// statusRecorder captures the status code a handler writes, so the logging
// middleware can report it. Handlers that never call WriteHeader are
// recorded as 200, matching net/http's behaviour.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}

func (s *statusRecorder) Write(p []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(p)
}

// loggingMiddleware writes one structured log line per request when it
// completes: method, path, status, duration, and the request-scoped
// correlation ID. The logger is request-scoped only in the sense that
// these fields travel with the log line; the logger itself is shared.
// A nil logger falls back to slog.Default().
func loggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			logger.Info("http request",
				"request_id", RequestIDFromContext(r.Context()),
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}

// tracingMiddleware starts one span per request, named after the method
// and path, and ends it when the handler returns. With the default
// no-op tracer this costs a context value and nothing else.
func tracingMiddleware(tracer tracing.Tracer) func(http.Handler) http.Handler {
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, span := tracer.Start(r.Context(), "http "+r.Method+" "+r.URL.Path)
			defer span.End()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RateLimitConfig configures the posting rate limiter. A zero
// RequestsPerSecond disables limiting.
type RateLimitConfig struct {
	RequestsPerSecond float64 // sustained rate, per client IP
	Burst             int     // bucket size: short bursts up to this many
}

// RateLimiter is a per-key token bucket. Keys are client IPs, so one
// misbehaving caller cannot starve others — the failure mode of a
// single global bucket. It is intentionally not a distributed limiter:
// each service instance limits independently, which is the correct
// trade-off until the service runs behind shared state.
type RateLimiter struct {
	rate    float64
	burst   float64
	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

type tokenBucket struct {
	tokens   float64
	lastSeen time.Time
}

// NewRateLimiter returns a limiter for cfg, or nil when cfg disables
// limiting (zero rate). A non-positive burst is raised to 1 so a
// configured rate always admits at least one request per refill.
func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	if cfg.RequestsPerSecond <= 0 {
		return nil
	}
	burst := float64(cfg.Burst)
	if burst < 1 {
		burst = 1
	}
	return &RateLimiter{
		rate:    cfg.RequestsPerSecond,
		burst:   burst,
		buckets: make(map[string]*tokenBucket),
	}
}

// Allow reports whether key may proceed, consuming one token. Tokens
// refill continuously at the configured rate; a bucket that has been
// idle for over a minute is dropped on the next Allow call that notices
// the map growing, so the map cannot grow unboundedly with client IPs.
func (l *RateLimiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	bucket, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) > 10000 {
			for k, b := range l.buckets {
				if now.Sub(b.lastSeen) > time.Minute {
					delete(l.buckets, k)
				}
			}
		}
		bucket = &tokenBucket{tokens: l.burst, lastSeen: now}
		l.buckets[key] = bucket
	}
	elapsed := now.Sub(bucket.lastSeen).Seconds()
	bucket.tokens += elapsed * l.rate
	if bucket.tokens > l.burst {
		bucket.tokens = l.burst
	}
	bucket.lastSeen = now
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

// clientIP extracts the remote IP without the port. X-Forwarded-For is
// deliberately ignored: the service does not know whether it sits behind
// a trusted proxy, and trusting a client-controlled header would let a
// caller rotate identities past the limiter for free.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// rateLimitMiddleware applies the limiter to POST /transactions only —
// the write path that needs protection from retry storms and runaway
// callers. Reads are unmetered: they are cheap, derived reads, and
// limiting them would punish legitimate balance polling. A denied
// request gets 429 with a Retry-After hint and the same JSON error shape
// as the rest of the API.
func rateLimitMiddleware(limiter *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limiter != nil && r.Method == http.MethodPost && r.URL.Path == "/transactions" {
				if !limiter.Allow(clientIP(r)) {
					w.Header().Set("Retry-After", "1")
					writeJSON(w, http.StatusTooManyRequests, errorJSON{Error: "rate limit exceeded", Code: "rate_limited"})
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
