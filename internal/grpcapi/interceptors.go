package grpcapi

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tandrysyawaludin/meridian-ledger/internal/metrics"
	"github.com/tandrysyawaludin/meridian-ledger/internal/tracing"
)

// Interceptors in this file mirror the HTTP middleware (see
// internal/api/middleware.go) so both transports report the same signals:
// one structured log line per call, posting latency and rejections on the
// shared metrics registry, and one span per call on the configured tracer.

// UnaryLoggingInterceptor logs one structured line per gRPC call: method,
// status code, and duration. A nil logger falls back to slog.Default().
func UnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.Info("grpc request",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return resp, err
	}
}

// UnaryMetricsInterceptor records posting latency and rejections for
// PostTransaction calls on the shared registry. Other methods pass
// through unmeasured: the metrics story is about the write path, and
// measuring reads would dilute the posting histogram. A nil registry is
// a no-op, so the interceptor is safe to install unconditionally.
//
// The rejection reason comes from the gRPC status code, not the domain
// error: the handler converts domain errors to status errors before the
// interceptor sees them, and status codes are coarser (every validation
// failure is InvalidArgument). So a gRPC rejection reports "validation"
// where the HTTP path reports the finer "unbalanced" — same series, less
// granularity, and never mislabelled.
func UnaryMetricsInterceptor(reg *metrics.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !strings.HasSuffix(info.FullMethod, "/PostTransaction") {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		reg.ObservePostOutcome(time.Since(start), err == nil, grpcRejectionReason(err))
		return resp, err
	}
}

// grpcRejectionReason maps a handler error's status code to a rejection
// reason. It returns "" for a nil error.
func grpcRejectionReason(err error) string {
	if err == nil {
		return ""
	}
	switch status.Code(err) {
	case codes.NotFound:
		return "not_found"
	case codes.AlreadyExists:
		return "duplicate_account"
	case codes.FailedPrecondition:
		return "idempotency_conflict"
	case codes.InvalidArgument:
		return "validation"
	default:
		return "internal"
	}
}

// UnaryTracingInterceptor starts one span per call, named after the
// method, and ends it when the handler returns. A nil tracer means the
// no-op tracer.
func UnaryTracingInterceptor(tracer tracing.Tracer) grpc.UnaryServerInterceptor {
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, span := tracer.Start(ctx, "grpc "+info.FullMethod)
		defer span.End()
		return handler(ctx, req)
	}
}
