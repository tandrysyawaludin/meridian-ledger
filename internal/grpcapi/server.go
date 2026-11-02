// Package grpcapi adapts the ledger service to the generated gRPC
// surface (see proto/ledger/v1/ledger.proto). It mirrors the HTTP API
// exactly: same domain calls, same error semantics, mapped to gRPC status
// codes (InvalidArgument for validation, NotFound for unknown accounts or
// transactions, AlreadyExists / FailedPrecondition for duplicate accounts
// and idempotency conflicts).
package grpcapi

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/tandrysyawaludin/meridian-ledger/gen/ledger/v1"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
)

// Server implements ledgerv1.LedgerServiceServer on top of a ledger.Service.
type Server struct {
	ledgerv1.UnimplementedLedgerServiceServer
	svc *ledger.Service
}

// NewServer returns a gRPC server implementation backed by svc.
func NewServer(svc *ledger.Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error) {
	account, err := s.svc.CreateAccount(ctx, ledger.Account{
		ID: req.GetId(), Name: req.GetName(), Currency: req.GetCurrency(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return toAccountProto(account), nil
}

func (s *Server) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.Account, error) {
	account, err := s.svc.Account(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return toAccountProto(account), nil
}

func (s *Server) PostTransaction(ctx context.Context, req *ledgerv1.PostTransactionRequest) (*ledgerv1.Transaction, error) {
	entries := make([]ledger.Entry, len(req.GetEntries()))
	for i, e := range req.GetEntries() {
		entries[i] = ledger.Entry{
			AccountID: e.GetAccountId(),
			Direction: ledger.Direction(e.GetDirection()),
			Amount:    e.GetAmountMinor(),
		}
	}
	tx, err := s.svc.Post(ctx, ledger.PostRequest{
		IdempotencyKey: req.GetIdempotencyKey(), Memo: req.GetMemo(), Entries: entries,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return toTransactionProto(tx), nil
}

func (s *Server) GetTransaction(ctx context.Context, req *ledgerv1.GetTransactionRequest) (*ledgerv1.Transaction, error) {
	tx, err := s.svc.Transaction(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return toTransactionProto(tx), nil
}

func (s *Server) GetBalance(ctx context.Context, req *ledgerv1.GetBalanceRequest) (*ledgerv1.Balance, error) {
	snap, err := s.svc.BalanceSnapshot(ctx, req.GetAccountId())
	if err != nil {
		return nil, toStatus(err)
	}
	account, err := s.svc.Account(ctx, req.GetAccountId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &ledgerv1.Balance{
		AccountId:    snap.AccountID,
		BalanceMinor: snap.Balance,
		Currency:     account.Currency,
		Version:      snap.Version,
	}, nil
}

func toAccountProto(a ledger.Account) *ledgerv1.Account {
	return &ledgerv1.Account{
		Id:        a.ID,
		Name:      a.Name,
		Currency:  a.Currency,
		CreatedAt: a.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func toTransactionProto(tx ledger.Transaction) *ledgerv1.Transaction {
	entries := make([]*ledgerv1.Entry, len(tx.Entries))
	for i, e := range tx.Entries {
		entries[i] = &ledgerv1.Entry{
			AccountId:   e.AccountID,
			Direction:   string(e.Direction),
			AmountMinor: e.Amount,
		}
	}
	return &ledgerv1.Transaction{
		Id:             tx.ID,
		IdempotencyKey: tx.IdempotencyKey,
		Memo:           tx.Memo,
		Entries:        entries,
		CreatedAt:      tx.CreatedAt.UTC().Format(time.RFC3339),
	}
}

// toStatus maps domain errors to gRPC codes; anything unrecognised is
// Internal with no detail leaked.
func toStatus(err error) error {
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound) || errors.Is(err, ledger.ErrTransactionNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ledger.ErrAccountExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ledger.ErrEmptyIdempotencyKey),
		errors.Is(err, ledger.ErrTooFewEntries),
		errors.Is(err, ledger.ErrInvalidDirection),
		errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrUnbalanced),
		errors.Is(err, ledger.ErrMixedCurrencies),
		errors.Is(err, ledger.ErrEmptyAccountID),
		errors.Is(err, ledger.ErrInvalidAccount):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
