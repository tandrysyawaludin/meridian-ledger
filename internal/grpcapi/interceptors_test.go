package grpcapi_test

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ledgerv1 "github.com/tandrysyawaludin/meridian-ledger/gen/ledger/v1"
	"github.com/tandrysyawaludin/meridian-ledger/internal/grpcapi"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/metrics"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

// newInterceptedClient is newTestClient with the Day 4 interceptors
// installed, so the metrics the interceptors record can be scraped.
func newInterceptedClient(t *testing.T, reg *metrics.Registry) ledgerv1.LedgerServiceClient {
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
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpcapi.UnaryLoggingInterceptor(nil),
		grpcapi.UnaryMetricsInterceptor(reg),
	))
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

func TestGRPCMetricsInterceptorRecordsPostings(t *testing.T) {
	reg := metrics.NewRegistry()
	client := newInterceptedClient(t, reg)
	ctx := context.Background()

	entries := []*ledgerv1.Entry{
		{AccountId: "cash", Direction: "debit", AmountMinor: 100},
		{AccountId: "revenue", Direction: "credit", AmountMinor: 100},
	}
	if _, err := client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "grpc-m-1", Entries: entries,
	}); err != nil {
		t.Fatalf("success post: %v", err)
	}
	// Unbalanced posting must be recorded as a rejection, not dropped.
	// Note the reason is "validation", not "unbalanced": the handler
	// converts the domain error to an InvalidArgument status before the
	// interceptor sees it (see grpcRejectionReason).
	bad := []*ledgerv1.Entry{
		{AccountId: "cash", Direction: "debit", AmountMinor: 100},
		{AccountId: "revenue", Direction: "credit", AmountMinor: 50},
	}
	if _, err := client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "grpc-m-2", Entries: bad,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unbalanced post: got code %v, want InvalidArgument", status.Code(err))
	}
	// A read must not touch the posting series.
	if _, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: "cash"}); err != nil {
		t.Fatalf("get account: %v", err)
	}

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`meridian_postings_total{outcome="success"} 1`,
		`meridian_postings_total{outcome="rejected"} 1`,
		`meridian_posting_rejections_total{reason="validation"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
}
