package tracing_test

import (
	"context"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/tracing"
)

func TestNoopTracerRoundTrip(t *testing.T) {
	var tracer tracing.Tracer = tracing.NoopTracer{}
	ctx, span := tracer.Start(context.Background(), "test")
	if span == nil {
		t.Fatal("noop tracer returned a nil span")
	}
	span.SetAttribute("key", "value")
	span.End()
	span.End() // ending twice must be safe
	_ = ctx
}

func TestContextSpanHelpers(t *testing.T) {
	if got := tracing.SpanFromContext(context.Background()); got != nil {
		t.Errorf("empty context should carry no span, got %v", got)
	}
	ctx, span := tracing.NoopTracer{}.Start(context.Background(), "x")
	_ = ctx
	ctx2 := tracing.ContextWithSpan(context.Background(), span)
	if tracing.SpanFromContext(ctx2) != span {
		t.Error("span stored in context was not retrieved")
	}
}

func TestNewID(t *testing.T) {
	a, b := tracing.NewID(), tracing.NewID()
	if len(a) != 32 || len(b) != 32 {
		t.Fatalf("IDs should be 32 hex chars, got %q and %q", a, b)
	}
	if a == b {
		t.Error("two generated IDs collided")
	}
}
