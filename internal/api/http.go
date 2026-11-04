// Package api exposes the ledger service over HTTP as a JSON API.
//
// Endpoints (amounts are integer minor units throughout):
//
//	POST /accounts                 create an account
//	GET  /accounts/{id}            fetch one account
//	GET  /accounts/{id}/balance    balance + optimistic-concurrency version
//	POST /transactions             post a balanced transaction
//	GET  /transactions/{id}        fetch one transaction
//
// Status codes map from domain errors: 400 for validation failures, 404
// for unknown accounts or transactions, 409 for duplicate accounts and
// idempotency-key conflicts, 500 for anything else. Error bodies are
// JSON: {"error": "...", "code": "..."}.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/metrics"
	"github.com/tandrysyawaludin/meridian-ledger/internal/tracing"
)

// Handler serves the ledger HTTP API.
type Handler struct {
	svc     *ledger.Service
	mux     *http.ServeMux
	metrics *metrics.Registry
	logger  *slog.Logger
	tracer  tracing.Tracer
	limiter *RateLimiter
	chain   http.Handler
}

// Options configures a Handler. Zero values are safe: metrics default to
// a fresh registry, logging to slog.Default, tracing to the no-op tracer,
// and rate limiting to disabled.
type Options struct {
	Metrics   *metrics.Registry
	Logger    *slog.Logger
	Tracer    tracing.Tracer
	RateLimit RateLimitConfig
}

// NewHandler returns an http.Handler serving the ledger API on top of svc,
// with default observability options (fresh metrics registry, default
// logger, no-op tracing, no rate limiting).
func NewHandler(svc *ledger.Service) http.Handler {
	return NewHandlerWithOptions(svc, Options{})
}

// NewHandlerWithOptions returns a Handler like NewHandler but with the
// given observability wiring. The returned handler's Metrics registry is
// the one callers should expose on /metrics; see Metrics().
func NewHandlerWithOptions(svc *ledger.Service, opts Options) *Handler {
	h := &Handler{
		svc:     svc,
		mux:     http.NewServeMux(),
		metrics: opts.Metrics,
		logger:  opts.Logger,
		tracer:  opts.Tracer,
		limiter: NewRateLimiter(opts.RateLimit),
	}
	if h.metrics == nil {
		h.metrics = metrics.NewRegistry()
	}
	if h.logger == nil {
		h.logger = slog.Default()
	}
	if h.tracer == nil {
		h.tracer = tracing.NoopTracer{}
	}
	h.mux.HandleFunc("POST /accounts", h.createAccount)
	h.mux.HandleFunc("GET /accounts/{id}", h.getAccount)
	h.mux.HandleFunc("GET /accounts/{id}/balance", h.getBalance)
	h.mux.HandleFunc("POST /transactions", h.postTransaction)
	h.mux.HandleFunc("GET /transactions/{id}", h.getTransaction)
	// Middleware order is deliberate: request ID first so every later
	// layer (logging, rate-limit responses) can carry it.
	h.chain = requestIDMiddleware(
		loggingMiddleware(h.logger)(
			tracingMiddleware(h.tracer)(
				rateLimitMiddleware(h.limiter)(h.mux))))
	return h
}

// Metrics returns the registry this handler records into, for mounting on
// /metrics by the service entry point.
func (h *Handler) Metrics() *metrics.Registry { return h.metrics }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.chain.ServeHTTP(w, r)
}

// JSON wire shapes. Field names match the protobuf surface (snake_case)
// so HTTP and gRPC clients see the same contract.

type accountJSON struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Currency  string `json:"currency"`
	CreatedAt string `json:"created_at"`
}

type entryJSON struct {
	AccountID   string `json:"account_id"`
	Direction   string `json:"direction"`
	AmountMinor int64  `json:"amount_minor"`
}

type transactionJSON struct {
	ID             string      `json:"id"`
	IdempotencyKey string      `json:"idempotency_key"`
	Memo           string      `json:"memo"`
	Entries        []entryJSON `json:"entries"`
	CreatedAt      string      `json:"created_at"`
}

type balanceJSON struct {
	AccountID    string `json:"account_id"`
	BalanceMinor int64  `json:"balance_minor"`
	Currency     string `json:"currency"`
	Version      int64  `json:"version"`
}

type createAccountRequest struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Currency string `json:"currency"`
}

type postTransactionRequest struct {
	IdempotencyKey string      `json:"idempotency_key"`
	Memo           string      `json:"memo"`
	Entries        []entryJSON `json:"entries"`
}

type errorJSON struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func toAccountJSON(a ledger.Account) accountJSON {
	return accountJSON{ID: a.ID, Name: a.Name, Currency: a.Currency, CreatedAt: a.CreatedAt.UTC().Format(time.RFC3339)}
}

func toTransactionJSON(tx ledger.Transaction) transactionJSON {
	entries := make([]entryJSON, len(tx.Entries))
	for i, e := range tx.Entries {
		entries[i] = entryJSON{AccountID: e.AccountID, Direction: string(e.Direction), AmountMinor: e.Amount}
	}
	return transactionJSON{
		ID:             tx.ID,
		IdempotencyKey: tx.IdempotencyKey,
		Memo:           tx.Memo,
		Entries:        entries,
		CreatedAt:      tx.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func (h *Handler) createAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	account, err := h.svc.CreateAccount(r.Context(), ledger.Account{
		ID: req.ID, Name: req.Name, Currency: req.Currency,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toAccountJSON(account))
}

func (h *Handler) getAccount(w http.ResponseWriter, r *http.Request) {
	account, err := h.svc.Account(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toAccountJSON(account))
}

func (h *Handler) getBalance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	snap, err := h.svc.BalanceSnapshot(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	account, err := h.svc.Account(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, balanceJSON{
		AccountID:    snap.AccountID,
		BalanceMinor: snap.Balance,
		Currency:     account.Currency,
		Version:      snap.Version,
	})
}

func (h *Handler) postTransaction(w http.ResponseWriter, r *http.Request) {
	var req postTransactionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	entries := make([]ledger.Entry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = ledger.Entry{AccountID: e.AccountID, Direction: ledger.Direction(e.Direction), Amount: e.AmountMinor}
	}
	start := time.Now()
	tx, err := h.svc.Post(r.Context(), ledger.PostRequest{
		IdempotencyKey: req.IdempotencyKey, Memo: req.Memo, Entries: entries,
	})
	// Metrics cover every posting attempt, including rejections: a
	// rejection is the interesting signal, not something to hide from
	// the latency and rejection series.
	h.metrics.ObservePost(time.Since(start), err)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toTransactionJSON(tx))
}

func (h *Handler) getTransaction(w http.ResponseWriter, r *http.Request) {
	tx, err := h.svc.Transaction(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionJSON(tx))
}

// decodeJSON reads a bounded JSON body into dst. It reports malformed
// input as a 400 and returns false; callers must not continue on false.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20)) // 1 MiB cap
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, errorJSON{Error: "invalid request body: " + err.Error(), Code: "validation"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError maps domain errors to HTTP statuses; anything unrecognised
// is a 500 with no internal detail leaked.
func writeError(w http.ResponseWriter, err error) {
	var status int
	var code string
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound) || errors.Is(err, ledger.ErrTransactionNotFound):
		status, code = http.StatusNotFound, "not_found"
	case errors.Is(err, ledger.ErrAccountExists) || errors.Is(err, ledger.ErrIdempotencyConflict):
		status, code = http.StatusConflict, "conflict"
	case errors.Is(err, ledger.ErrEmptyIdempotencyKey),
		errors.Is(err, ledger.ErrTooFewEntries),
		errors.Is(err, ledger.ErrInvalidDirection),
		errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrUnbalanced),
		errors.Is(err, ledger.ErrMixedCurrencies),
		errors.Is(err, ledger.ErrEmptyAccountID),
		errors.Is(err, ledger.ErrInvalidAccount):
		status, code = http.StatusBadRequest, "validation"
	default:
		status, code = http.StatusInternalServerError, "internal"
	}
	if status == http.StatusInternalServerError {
		writeJSON(w, status, errorJSON{Error: "internal error", Code: code})
		return
	}
	writeJSON(w, status, errorJSON{Error: err.Error(), Code: code})
}
