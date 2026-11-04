// Command meridian runs the Meridian Ledger service.
//
// HTTP JSON API and gRPC API on top of the ledger service, backed by the
// in-memory store by default or by Postgres when DATABASE_URL is set.
// Health endpoints live on the HTTP server, and Prometheus metrics on
// GET /metrics. When KAFKA_BROKERS is set, the transactional-outbox relay
// also runs, publishing posted transactions to Kafka/Redpanda (see
// docs/EVENTS.md). Observability details — metrics series, log fields,
// tracing hooks, rate limiting — are documented in docs/OPERABILITY.md.
//
// Configuration:
//
//	PORT                  HTTP listen port (default 8080)
//	GRPC_PORT             gRPC listen port (default 9090)
//	DATABASE_URL          Postgres connection string; when unset, the in-memory
//	                      store is used (suitable for local development and tests)
//	KAFKA_BROKERS         Comma-separated Kafka/Redpanda brokers; when unset, the
//	                      outbox relay does not run and events stay unpublished
//	KAFKA_TOPIC           Event topic (default meridian-ledger-events)
//	POST_RATE_LIMIT_RPS   Sustained posting rate per client IP (default 100;
//	                      0 disables rate limiting)
//	POST_RATE_LIMIT_BURST Posting burst allowance per client IP (default 200)
//
// Shutdown is coordinated, in this order: stop accepting new work (HTTP
// shutdown drains in-flight requests, gRPC graceful stop drains in-flight
// calls), stop the outbox relay and wait for it, close the publisher, then
// close the store. The order matters: a posting that is in flight when
// the signal arrives still commits its outbox rows, and the relay is only
// stopped after the servers have drained, so no committed event is left
// unpublished by a relay that was killed mid-batch. Anything still
// unpublished after shutdown is picked up by the next run, in order.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	ledgerv1 "github.com/tandrysyawaludin/meridian-ledger/gen/ledger/v1"
	"github.com/tandrysyawaludin/meridian-ledger/internal/api"
	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/grpcapi"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/metrics"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
	"github.com/tandrysyawaludin/meridian-ledger/internal/tracing"
)

// outboxStatsStore is the slice of the store contract the lag reporter
// needs. Both backends implement it; the assertion is checked at startup
// so a backend that ever drops it fails loudly instead of silently
// reporting a zero backlog.
type outboxStatsStore interface {
	OutboxStats(ctx context.Context) (pending int, oldest time.Time, found bool, err error)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := envOr("PORT", "8080")
	grpcPort := envOr("GRPC_PORT", "9090")
	rateLimit := api.RateLimitConfig{
		RequestsPerSecond: envFloat("POST_RATE_LIMIT_RPS", 100),
		Burst:             envInt("POST_RATE_LIMIT_BURST", 200),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Backend selection: Postgres when configured, otherwise the
	// in-memory store. Either way the domain service is identical, and
	// both backends implement the outbox port the relay drains.
	reg := metrics.NewRegistry()
	var svc *ledger.Service
	var outbox events.OutboxStore
	var statsStore outboxStatsStore
	var closeStore func()
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		pg, err := store.NewPostgresStore(ctx, dsn)
		if err != nil {
			logger.Error("postgres store failed", "err", err)
			os.Exit(1)
		}
		closeStore = pg.Close
		svc = ledger.NewService(pg)
		outbox = pg
		statsStore = pg
		logger.Info("using postgres store")
	} else {
		mem := store.NewMemoryStore()
		svc = ledger.NewService(mem)
		outbox = mem
		statsStore = mem
		logger.Info("using in-memory store (set DATABASE_URL for Postgres)")
	}
	if closeStore != nil {
		defer closeStore()
	}

	// Outbox lag reporter: polls the backlog every 5 seconds and feeds
	// the outbox gauges. It runs on its own context so shutdown can stop
	// it without waiting for a poll, and it never fails startup: a stats
	// error is logged and retried on the next tick.
	lagCtx, stopLag := context.WithCancel(context.Background())
	var lagDone sync.WaitGroup
	lagDone.Add(1)
	go func() {
		defer lagDone.Done()
		reportOutboxLag(lagCtx, logger, reg, statsStore)
	}()

	// Outbox relay: only when a broker is configured. Without one, events
	// accumulate unpublished in the outbox and a later run with a broker
	// publishes them in order — nothing is lost by running brokerless.
	// The relay runs on its own context, cancelled during shutdown after
	// the servers have drained (see below).
	relayCtx, stopRelay := context.WithCancel(context.Background())
	var relayDone chan struct{}
	var closePublisher func()
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		topic := envOr("KAFKA_TOPIC", "meridian-ledger-events")
		publisher, err := events.NewKafkaPublisher(strings.Split(brokers, ","), topic)
		if err != nil {
			logger.Error("kafka publisher failed", "err", err)
			os.Exit(1)
		}
		closePublisher = func() { _ = publisher.Close() }
		relay := &events.Relay{Store: outbox, Publisher: publisher, Logger: logger}
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			relay.Run(relayCtx)
		}()
		logger.Info("outbox relay running", "topic", topic)
	} else {
		logger.Info("outbox relay disabled (set KAFKA_BROKERS to publish events)")
	}

	tracer := tracing.NoopTracer{}
	handler := api.NewHandlerWithOptions(svc, api.Options{
		Metrics:   reg,
		Logger:    logger,
		Tracer:    tracer,
		RateLimit: rateLimit,
	})

	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", handler))
	mux.HandleFunc("GET /healthz", healthHandler("ok"))
	mux.HandleFunc("GET /readyz", healthHandler("ready"))
	mux.Handle("GET /metrics", reg.Handler())

	httpSrv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Error("grpc listen failed", "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpcapi.UnaryLoggingInterceptor(logger),
		grpcapi.UnaryTracingInterceptor(tracer),
		grpcapi.UnaryMetricsInterceptor(reg),
	))
	ledgerv1.RegisterLedgerServiceServer(grpcSrv, grpcapi.NewServer(svc))

	go func() {
		logger.Info("meridian ledger listening", "http_addr", httpSrv.Addr, "grpc_addr", grpcLis.Addr().String())
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()
	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Error("grpc server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	// 1. Stop accepting new work and drain in-flight requests/calls.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", "err", err)
		}
	}()
	go func() {
		defer wg.Done()
		done := make(chan struct{})
		go func() { grpcSrv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			grpcSrv.Stop()
		}
	}()
	wg.Wait()

	// 2. Stop the lag reporter and the relay, and wait for the relay to
	// finish its in-flight poll. The relay stops polling on its own
	// context; a stuck publish is bounded by the wait below, not by
	// process exit, so shutdown cannot hang forever on a dead broker.
	stopLag()
	lagDone.Wait()
	stopRelay()
	if relayDone != nil {
		select {
		case <-relayDone:
		case <-time.After(10 * time.Second):
			logger.Warn("outbox relay did not stop in time")
		}
	}

	// 3. Close the publisher, then the store. Nothing publishes or reads
	// after this point.
	if closePublisher != nil {
		closePublisher()
	}
	logger.Info("stopped")
}

// reportOutboxLag polls statsStore every 5 seconds and feeds the outbox
// gauges. A stats error is logged and retried; it must never crash the
// service, since metrics are observability, not the write path.
func reportOutboxLag(ctx context.Context, logger *slog.Logger, reg *metrics.Registry, statsStore outboxStatsStore) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		pending, oldest, found, err := statsStore.OutboxStats(ctx)
		if err != nil {
			logger.Warn("outbox stats failed", "err", err)
		} else if found {
			reg.SetOutboxLag(pending, time.Since(oldest))
		} else {
			reg.SetOutboxLag(0, 0)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func healthHandler(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"meridian-ledger","status":"` + status + `"}` + "\n"))
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
