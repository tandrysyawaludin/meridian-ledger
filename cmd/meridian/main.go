// Command meridian runs the Meridian Ledger service.
//
// HTTP JSON API and gRPC API on top of the ledger service, backed by the
// in-memory store by default or by Postgres when DATABASE_URL is set.
// Health endpoints live on the HTTP server. When KAFKA_BROKERS is set,
// the transactional-outbox relay also runs, publishing posted
// transactions to Kafka/Redpanda (see docs/EVENTS.md).
//
// Configuration:
//
//	PORT          HTTP listen port (default 8080)
//	GRPC_PORT     gRPC listen port (default 9090)
//	DATABASE_URL  Postgres connection string; when unset, the in-memory
//	              store is used (suitable for local development and tests)
//	KAFKA_BROKERS Comma-separated Kafka/Redpanda brokers; when unset, the
//	              outbox relay does not run and events stay unpublished
//	KAFKA_TOPIC   Event topic (default meridian-ledger-events)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	ledgerv1 "github.com/tandrysyawaludin/meridian-ledger/gen/ledger/v1"
	"github.com/tandrysyawaludin/meridian-ledger/internal/api"
	"github.com/tandrysyawaludin/meridian-ledger/internal/events"
	"github.com/tandrysyawaludin/meridian-ledger/internal/grpcapi"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9090"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Backend selection: Postgres when configured, otherwise the
	// in-memory store. Either way the domain service is identical, and
	// both backends implement the outbox port the relay drains.
	var svc *ledger.Service
	var outbox events.OutboxStore
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
		logger.Info("using postgres store")
	} else {
		mem := store.NewMemoryStore()
		svc = ledger.NewService(mem)
		outbox = mem
		logger.Info("using in-memory store (set DATABASE_URL for Postgres)")
	}
	if closeStore != nil {
		defer closeStore()
	}

	// Outbox relay: only when a broker is configured. Without one, events
	// accumulate unpublished in the outbox and a later run with a broker
	// publishes them in order — nothing is lost by running brokerless.
	var relayDone chan struct{}
	var closePublisher func()
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		topic := os.Getenv("KAFKA_TOPIC")
		if topic == "" {
			topic = "meridian-ledger-events"
		}
		publisher, err := events.NewKafkaPublisher(strings.Split(brokers, ","), topic)
		if err != nil {
			logger.Error("kafka publisher failed", "err", err)
			os.Exit(1)
		}
		closePublisher = func() { _ = publisher.Close() }
		relay := &events.Relay{Store: outbox, Publisher: publisher}
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			relay.Run(ctx)
		}()
		logger.Info("outbox relay running", "topic", topic)
	} else {
		logger.Info("outbox relay disabled (set KAFKA_BROKERS to publish events)")
	}

	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", api.NewHandler(svc)))
	mux.HandleFunc("GET /healthz", healthHandler("ok"))
	mux.HandleFunc("GET /readyz", healthHandler("ready"))

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
	grpcSrv := grpc.NewServer()
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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcSrv.GracefulStop()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
		os.Exit(1)
	}
	// The relay stops on the same signal context; give it a moment to
	// finish its in-flight poll before closing the publisher. (Day 4
	// hardens this into a fully coordinated shutdown.)
	if relayDone != nil {
		select {
		case <-relayDone:
		case <-time.After(5 * time.Second):
			logger.Warn("outbox relay did not stop in time")
		}
	}
	if closePublisher != nil {
		closePublisher()
	}
	logger.Info("stopped")
}

func healthHandler(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"meridian-ledger","status":"` + status + `"}` + "\n"))
	}
}
