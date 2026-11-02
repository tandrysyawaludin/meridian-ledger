// Command meridian runs the Meridian Ledger service.
//
// Day 2 scope: HTTP JSON API and gRPC API on top of the ledger service,
// backed by the in-memory store by default or by Postgres when
// DATABASE_URL is set. Health endpoints live on the HTTP server.
//
// Configuration:
//
//	PORT          HTTP listen port (default 8080)
//	GRPC_PORT     gRPC listen port (default 9090)
//	DATABASE_URL  Postgres connection string; when unset, the in-memory
//	              store is used (suitable for local development and tests)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	ledgerv1 "github.com/tandrysyawaludin/meridian-ledger/gen/ledger/v1"
	"github.com/tandrysyawaludin/meridian-ledger/internal/api"
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
	// in-memory store. Either way the domain service is identical.
	var svc *ledger.Service
	var closeStore func()
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		pg, err := store.NewPostgresStore(ctx, dsn)
		if err != nil {
			logger.Error("postgres store failed", "err", err)
			os.Exit(1)
		}
		closeStore = pg.Close
		svc = ledger.NewService(pg)
		logger.Info("using postgres store")
	} else {
		svc = ledger.NewService(store.NewMemoryStore())
		logger.Info("using in-memory store (set DATABASE_URL for Postgres)")
	}
	if closeStore != nil {
		defer closeStore()
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
	logger.Info("stopped")
}

func healthHandler(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"meridian-ledger","status":"` + status + `"}` + "\n"))
	}
}
