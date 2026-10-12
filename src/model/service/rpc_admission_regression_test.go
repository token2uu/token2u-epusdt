package service

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/spf13/viper"
)

func TestRPCEndpointQueueDoesNotCompressGlobalSolanaBudget(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	isolateSolThrottle(t, 2)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	viper.Set("rpc_public_requests_per_second", 1)
	var mu sync.Mutex
	var requests []time.Time
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, time.Now())
		mu.Unlock()
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
	})
	slow, other := httptest.NewServer(handler), httptest.NewServer(handler)
	defer slow.Close()
	defer other.Close()
	if _, err := solRetryClientWithURL(slow.URL, "getHealth", nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := solRetryClientWithURL(slow.URL, "getHealth", nil)
		done <- err
	}()
	// At 2 global RPS the second call can enter the endpoint's 1 RPS queue
	// after 500 ms. A call to another endpoint then catches it at one second.
	time.Sleep(650 * time.Millisecond)
	if _, err := solRetryClientWithURL(other.URL, "getHealth", nil); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].Before(requests[j]) })
	if len(requests) != 3 {
		t.Fatalf("calls=%d", len(requests))
	}
	if gap := requests[2].Sub(requests[1]); gap < 400*time.Millisecond {
		t.Fatalf("endpoint queue compressed global 2 RPS spacing: last gap=%s, total span=%s", gap, requests[2].Sub(requests[0]))
	}
}
