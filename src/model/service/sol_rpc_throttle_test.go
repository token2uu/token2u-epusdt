package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func isolateSolThrottle(t *testing.T, rps float64) {
	t.Helper()
	old := solRPCThrottle
	solRPCThrottle = &solanaRPCThrottle{cooldowns: make(map[string]time.Time)}
	previous := viper.Get("solana_rpc_requests_per_second")
	viper.Set("solana_rpc_requests_per_second", rps)
	t.Cleanup(func() {
		solRPCThrottle = old
		viper.Set("solana_rpc_requests_per_second", previous)
	})
}

func TestSolRPCPacesConcurrentClientsAndRetries(t *testing.T) {
	isolateSolThrottle(t, 40)
	oldCount, oldWait, oldMax := solRPCRetryCount, solRPCRetryWait, solRPCRetryMaxWait
	solRPCRetryCount, solRPCRetryWait, solRPCRetryMaxWait = 2, time.Millisecond, time.Millisecond
	t.Cleanup(func() { solRPCRetryCount, solRPCRetryWait, solRPCRetryMaxWait = oldCount, oldWait, oldMax })
	var mu sync.Mutex
	var requests []time.Time
	handler := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, time.Now())
		n := len(requests)
		mu.Unlock()
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
	}
	a, b := httptest.NewServer(http.HandlerFunc(handler)), httptest.NewServer(http.HandlerFunc(handler))
	defer a.Close()
	defer b.Close()
	var wg sync.WaitGroup
	for _, endpoint := range []string{a.URL, b.URL, a.URL} {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			if _, err := solRetryClientWithURL(endpoint, "getHealth", nil); err != nil {
				t.Errorf("RPC failed: %v", err)
			}
		}(endpoint)
	}
	wg.Wait()
	if len(requests) != 5 {
		t.Fatalf("requests = %d, want 3 calls + 2 retries", len(requests))
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].Before(requests[j]) })
	for i := 1; i < len(requests); i++ {
		if gap := requests[i].Sub(requests[i-1]); gap < 20*time.Millisecond {
			t.Fatalf("requests bypassed shared budget: gap %s", gap)
		}
	}
}

func TestSolRPC429CooldownThenRecovers(t *testing.T) {
	isolateSolThrottle(t, 1000)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
	}))
	defer server.Close()
	_, err := solRetryClientWithURL(server.URL, "getHealth", nil)
	var limited *solanaRateLimitError
	if !errors.As(err, &limited) {
		t.Fatalf("expected rate limit, got %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := solRetryClientWithURL(server.URL, "getHealth", nil); !errors.As(err, &limited) {
			t.Fatalf("expected shared cooldown, got %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("429 retried during cooldown: %d calls", calls.Load())
	}
	time.Sleep(time.Until(limited.until) + time.Millisecond)
	if _, err := solRetryClientWithURL(server.URL, "getHealth", nil); err != nil {
		t.Fatalf("did not recover after cooldown: %v", err)
	}
}

func TestSolRPC429ImmediatelyUsesBackup(t *testing.T) {
	var primaryCalls, backupCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer primary.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
	}))
	defer backup.Close()
	cleanup := setupSolanaRPCNode(t, primary.URL)
	defer cleanup()
	isolateSolThrottle(t, 1000)
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Url: backup.URL, Type: mdb.RpcNodeTypeHttp, Weight: 1, Enabled: true, Purpose: mdb.RpcNodePurposeGeneral, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := SolRetryClient("getHealth", nil); err != nil {
			t.Fatal(err)
		}
	}
	if primaryCalls.Load() != 1 || backupCalls.Load() != 2 {
		t.Fatalf("primary calls=%d backup calls=%d", primaryCalls.Load(), backupCalls.Load())
	}
}

func TestSolRPCCooldownHonorsHTTPDateAndDoesNotShorten(t *testing.T) {
	g := &solanaRPCThrottle{cooldowns: make(map[string]time.Time)}
	deadline := time.Now().Add(2 * time.Minute).Truncate(time.Second)
	first := g.rateLimited("endpoint", deadline.UTC().Format(http.TimeFormat))
	second := g.rateLimited("endpoint", "invalid")
	if !first.until.Equal(deadline) || !second.until.Equal(deadline) {
		t.Fatalf("cooldown shortened: %s / %s", first.until, second.until)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(g.wait(ctx, "other endpoint"), context.Canceled) {
		t.Fatal("throttle ignored canceled request")
	}
}
