package grpcapi_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ledgerv1 "github.com/tandrysyawaludin/meridian-ledger/gen/ledger/v1"
	"github.com/tandrysyawaludin/meridian-ledger/internal/grpcapi"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// newTestClient spins up the gRPC server over an in-memory bufconn and
// returns a client for it. No real network port is involved.
func newTestClient(t *testing.T) ledgerv1.LedgerServiceClient {
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
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	ledgerv1.RegisterLedgerServiceServer(server, grpcapi.NewServer(svc))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn)
}

func TestGRPCPostGetAndBalance(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	acct, err := client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
		Id: "bank", Name: "Bank", Currency: "USD",
	})
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if acct.GetId() != "bank" {
		t.Fatalf("CreateAccount: unexpected account %v", acct)
	}
	if _, err := client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
		Id: "bank", Name: "Bank", Currency: "USD",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate account: expected AlreadyExists, got %v", err)
	}

	tx, err := client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "sale-1",
		Memo:           "card sale",
		Entries: []*ledgerv1.Entry{
			{AccountId: "cash", Direction: "debit", AmountMinor: 5000},
			{AccountId: "revenue", Direction: "credit", AmountMinor: 5000},
		},
	})
	if err != nil {
		t.Fatalf("PostTransaction: %v", err)
	}

	// Idempotent replay returns the original transaction.
	replay, err := client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "sale-1",
		Memo:           "card sale",
		Entries: []*ledgerv1.Entry{
			{AccountId: "cash", Direction: "debit", AmountMinor: 5000},
			{AccountId: "revenue", Direction: "credit", AmountMinor: 5000},
		},
	})
	if err != nil || replay.GetId() != tx.GetId() {
		t.Fatalf("replay: err=%v id=%q (want %q)", err, replay.GetId(), tx.GetId())
	}

	// Unbalanced posting -> InvalidArgument.
	_, err = client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "bad-1",
		Entries: []*ledgerv1.Entry{
			{AccountId: "cash", Direction: "debit", AmountMinor: 5000},
			{AccountId: "revenue", Direction: "credit", AmountMinor: 4000},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unbalanced: expected InvalidArgument, got %v", err)
	}

	got, err := client.GetTransaction(ctx, &ledgerv1.GetTransactionRequest{Id: tx.GetId()})
	if err != nil || got.GetIdempotencyKey() != "sale-1" {
		t.Fatalf("GetTransaction: err=%v tx=%v", err, got)
	}
	if _, err := client.GetTransaction(ctx, &ledgerv1.GetTransactionRequest{Id: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing tx: expected NotFound, got %v", err)
	}

	bal, err := client.GetBalance(ctx, &ledgerv1.GetBalanceRequest{AccountId: "cash"})
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.GetBalanceMinor() != 5000 || bal.GetVersion() != 1 || bal.GetCurrency() != "USD" {
		t.Fatalf("GetBalance: unexpected %+v", bal)
	}
}
