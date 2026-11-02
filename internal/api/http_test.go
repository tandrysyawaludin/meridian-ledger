package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tandrysyawaludin/meridian-ledger/internal/api"
	"github.com/tandrysyawaludin/meridian-ledger/internal/ledger"
	"github.com/tandrysyawaludin/meridian-ledger/internal/store"
)

func newTestServer(t *testing.T) *httptest.Server {
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
	return httptest.NewServer(api.NewHandler(svc))
}

func do(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	var req *http.Request
	var err error
	if body == "" {
		req, err = http.NewRequest(method, url, nil)
	} else {
		req, err = http.NewRequest(method, url, bytes.NewBufferString(body))
	}
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp.StatusCode, decoded
}

func TestHTTPCreateAndGetAccount(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	status, body := do(t, "POST", srv.URL+"/accounts", `{"id":"bank","name":"Bank","currency":"USD"}`)
	if status != http.StatusCreated {
		t.Fatalf("create: got status %d, body %v", status, body)
	}
	if body["id"] != "bank" || body["currency"] != "USD" {
		t.Fatalf("create: unexpected body %v", body)
	}

	status, body = do(t, "GET", srv.URL+"/accounts/bank", "")
	if status != http.StatusOK || body["name"] != "Bank" {
		t.Fatalf("get: got status %d, body %v", status, body)
	}

	// Duplicate account -> 409.
	status, body = do(t, "POST", srv.URL+"/accounts", `{"id":"bank","name":"Bank","currency":"USD"}`)
	if status != http.StatusConflict || body["code"] != "conflict" {
		t.Fatalf("duplicate: got status %d, body %v", status, body)
	}

	// Unknown account -> 404.
	status, body = do(t, "GET", srv.URL+"/accounts/nope", "")
	if status != http.StatusNotFound || body["code"] != "not_found" {
		t.Fatalf("missing: got status %d, body %v", status, body)
	}
}

func TestHTTPPostGetAndBalance(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	status, body := do(t, "POST", srv.URL+"/transactions",
		`{"idempotency_key":"sale-1","memo":"card sale","entries":[
			{"account_id":"cash","direction":"debit","amount_minor":5000},
			{"account_id":"revenue","direction":"credit","amount_minor":5000}]}`)
	if status != http.StatusCreated {
		t.Fatalf("post: got status %d, body %v", status, body)
	}
	txID, ok := body["id"].(string)
	if !ok || txID == "" {
		t.Fatalf("post: missing transaction id in %v", body)
	}

	// Idempotent replay returns the original transaction, not a second one.
	status, replay := do(t, "POST", srv.URL+"/transactions",
		`{"idempotency_key":"sale-1","memo":"card sale","entries":[
			{"account_id":"cash","direction":"debit","amount_minor":5000},
			{"account_id":"revenue","direction":"credit","amount_minor":5000}]}`)
	if status != http.StatusCreated || replay["id"] != txID {
		t.Fatalf("replay: got status %d, body %v", status, replay)
	}

	// Same key, different payload -> 409.
	status, body = do(t, "POST", srv.URL+"/transactions",
		`{"idempotency_key":"sale-1","memo":"different","entries":[
			{"account_id":"cash","direction":"debit","amount_minor":6000},
			{"account_id":"revenue","direction":"credit","amount_minor":6000}]}`)
	if status != http.StatusConflict {
		t.Fatalf("conflict: got status %d, body %v", status, body)
	}

	// Unbalanced posting -> 400.
	status, body = do(t, "POST", srv.URL+"/transactions",
		`{"idempotency_key":"bad-1","entries":[
			{"account_id":"cash","direction":"debit","amount_minor":5000},
			{"account_id":"revenue","direction":"credit","amount_minor":4000}]}`)
	if status != http.StatusBadRequest || body["code"] != "validation" {
		t.Fatalf("unbalanced: got status %d, body %v", status, body)
	}

	// Fetch the transaction and check balances + versions.
	status, body = do(t, "GET", srv.URL+"/transactions/"+txID, "")
	if status != http.StatusOK || body["idempotency_key"] != "sale-1" {
		t.Fatalf("get tx: got status %d, body %v", status, body)
	}
	status, body = do(t, "GET", srv.URL+"/transactions/does-not-exist", "")
	if status != http.StatusNotFound {
		t.Fatalf("get missing tx: got status %d, body %v", status, body)
	}

	status, body = do(t, "GET", srv.URL+"/accounts/cash/balance", "")
	if status != http.StatusOK {
		t.Fatalf("balance: got status %d, body %v", status, body)
	}
	if body["balance_minor"] != float64(5000) {
		t.Fatalf("balance: expected 5000, got %v", body["balance_minor"])
	}
	if body["version"] != float64(1) {
		t.Fatalf("balance: expected version 1 after one posting, got %v", body["version"])
	}
	if body["currency"] != "USD" {
		t.Fatalf("balance: expected USD, got %v", body["currency"])
	}
}
