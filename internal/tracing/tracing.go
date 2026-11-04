// Package tracing defines the tracing seam for Meridian Ledger.
//
// The service ships with a no-op tracer: spans are created and ended, but
// nothing is exported, so tracing costs nothing unless an operator plugs
// in a real backend. The Tracer interface is deliberately shaped like the
// OpenTelemetry API (Start a span on a context, end it, carry it in the
// context) so wiring an OTel tracer later is a one-line swap in
// cmd/meridian, not a refactor of every call site. Callers must never
// assume a span is present: SpanFromContext returns nil when no tracing
// is configured, and every helper tolerates that.
//
// What gets traced today: one span per HTTP request and per gRPC call,
// created by the API-layer middleware. The ledger domain itself takes no
// tracer — its operations are synchronous and already covered by the
// request span — so a future domain-level span is an additive change.
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// Span is a single unit of traced work. Implementations record timing
// and attributes; the no-op implementation discards both.
type Span interface {
	// End finishes the span. It must be safe to call more than once.
	End()
	// SetAttribute records a key/value pair on the span. The value is
	// kept as-is; exporters decide how to render it.
	SetAttribute(key string, value any)
}

// Tracer starts spans. A Tracer must be safe for concurrent use.
type Tracer interface {
	// Start begins a span named name as a child of any span already in
	// ctx, and returns a context carrying the new span.
	Start(ctx context.Context, name string) (context.Context, Span)
}

type spanKey struct{}

// ContextWithSpan returns a context carrying span. It is what Start uses
// internally; exporters that create spans outside a Tracer can use it
// directly.
func ContextWithSpan(ctx context.Context, span Span) context.Context {
	return context.WithValue(ctx, spanKey{}, span)
}

// SpanFromContext returns the span in ctx, or nil when ctx carries none.
func SpanFromContext(ctx context.Context) Span {
	span, _ := ctx.Value(spanKey{}).(Span)
	return span
}

// NoopTracer is a Tracer that creates spans which record nothing. It is
// the default in cmd/meridian: tracing is available everywhere as an
// interface, and enabled nowhere until an operator provides a backend.
type NoopTracer struct{}

// Start returns ctx unchanged and a no-op span.
func (NoopTracer) Start(ctx context.Context, _ string) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End()                         {}
func (noopSpan) SetAttribute(_ string, _ any) {}

// NewID returns a random 128-bit identifier as hex, for request and trace
// correlation IDs. It never fails: on the (practically impossible)
// crypto/rand failure it falls back to a zero ID rather than panicking a
// request path over an identifier.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}
