// Command loadtest hammers a running Meridian Ledger HTTP API with
// concurrent postings and reports throughput and latency percentiles.
//
// It is a black-box harness: it talks to the service exactly like a real
// client, over HTTP, so the numbers include JSON encoding, routing,
// middleware, and the domain's posting path. Start the service first
// (in-memory store is fine for a throughput ceiling; Postgres numbers
// will be lower and are the ones that matter for capacity planning):
//
//	go run ./cmd/meridian &
//	go run ./cmd/loadtest -duration 30s -concurrency 32
//
// Every worker posts between the same two accounts with a unique
// idempotency key per posting, so no posting is ever a duplicate and no
// posting is ever rejected for idempotency. A non-2xx/409/429 response
// counts as an error and is reported, not hidden: a load test that
// swallows errors only measures the happy path.
//
// The harness is deliberately small: it measures the posting path, which
// is the path that must stay fast under load. Balance reads are cheap
// derived reads and are covered by the unit benchmarks instead.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

func main() {
	baseURL := flag.String("base-url", "http://localhost:8080", "service base URL (without /api suffix)")
	concurrency := flag.Int("concurrency", 16, "number of concurrent workers")
	duration := flag.Duration("duration", 10*time.Second, "how long to post")
	amount := flag.Int64("amount", 1000, "amount per posting, in minor units")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second}
	deadline := time.Now().Add(*duration)

	// Seed two accounts. They must exist before workers start, and the
	// harness must not fail silently if they cannot be created: a load
	// run against a half-seeded service measures error handling, not
	// throughput.
	for _, acct := range []struct{ id, name string }{
		{"load-cash", "Load Cash"},
		{"load-revenue", "Load Revenue"},
	} {
		if err := createAccount(client, *baseURL, acct.id, acct.name); err != nil {
			fmt.Fprintf(os.Stderr, "loadtest: seed account %s: %v\n", acct.id, err)
			os.Exit(1)
		}
	}

	var mu sync.Mutex
	var latencies []time.Duration
	var ok, rateLimited, other int
	runStart := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			seq := 0
			for time.Now().Before(deadline) {
				seq++
				start := time.Now()
				status, err := post(client, *baseURL, fmt.Sprintf("load-%d-%d", worker, seq), *amount)
				elapsed := time.Since(start)
				mu.Lock()
				latencies = append(latencies, elapsed)
				switch {
				case err != nil:
					other++
				case status == http.StatusCreated:
					ok++
				case status == http.StatusTooManyRequests:
					rateLimited++
				default:
					other++
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	total := ok + rateLimited + other
	if total == 0 {
		fmt.Println("loadtest: no requests completed")
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) time.Duration {
		idx := int(float64(len(latencies)-1) * p)
		return latencies[idx]
	}
	elapsed := time.Since(runStart)
	fmt.Printf("loadtest: %d requests in %s (%.1f req/s)\n", total, elapsed, float64(total)/elapsed.Seconds())
	fmt.Printf("  created: %d  rate-limited: %d  errors: %d\n", ok, rateLimited, other)
	fmt.Printf("  latency: p50=%s p95=%s p99=%s max=%s\n",
		percentile(0.50), percentile(0.95), percentile(0.99), latencies[len(latencies)-1])
	if other > 0 {
		fmt.Printf("loadtest: WARNING: %d requests failed with unexpected status or transport error\n", other)
	}
}

func createAccount(client *http.Client, baseURL, id, name string) error {
	body, _ := json.Marshal(map[string]string{"id": id, "name": name, "currency": "USD"})
	resp, err := client.Post(baseURL+"/api/accounts", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	// 409 means a previous run already created the account; that is a
	// fine starting state, not a failure.
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func post(client *http.Client, baseURL, key string, amount int64) (int, error) {
	body, _ := json.Marshal(map[string]any{
		"idempotency_key": key,
		"memo":            "load test",
		"entries": []map[string]any{
			{"account_id": "load-cash", "direction": "debit", "amount_minor": amount},
			{"account_id": "load-revenue", "direction": "credit", "amount_minor": amount},
		},
	})
	resp, err := client.Post(baseURL+"/api/transactions", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}
