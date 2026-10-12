package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

func TestRPCInFlightIndexerSnapshotDoesNotTriggerPaidFallback(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	data.ResetRpcRoutingStatsForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcFailoverForTest)
	t.Cleanup(data.ResetRpcRoutingStatsForTest)
	oldLimiter := aptosIndexerLimiter
	aptosIndexerLimiter = rate.NewLimiter(rate.Inf, 1)
	t.Cleanup(func() { aptosIndexerLimiter = oldLimiter })
	started, release := make(chan struct{}), make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	var publicHeads, paidCalls atomic.Int32
	var currentHead atomic.Int64
	currentHead.Store(111)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Query string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(req.Query, "fungible_asset_activities(") {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":110}],"fungible_asset_activities":[]}}`))
			return
		}
		publicHeads.Add(1)
		_, _ = fmt.Fprintf(w, `{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":%d}]}}`, currentHead.Load())
	}))
	defer primary.Close()
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paidCalls.Add(1)
		_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":111}],"fungible_asset_activities":[]}}`))
	}))
	defer paid.Close()
	primaryNode := mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: primary.URL, Purpose: mdb.RpcNodePurposeGeneral, Enabled: true, Status: mdb.RpcNodeStatusOk}
	paidNode := mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: paid.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}
	if err := dao.Mdb.Create(&primaryNode).Error; err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Create(&paidNode).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := AptosIndexerDeposits(ctx, "0xa", AptosEventPosition{Version: 100}, 110, 100)
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("deposit request did not start")
	}
	if err := ProbeRPCNode(primaryNode); err != nil {
		t.Fatal(err)
	}
	released.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := AptosIndexerHead(ctx); err != nil {
		t.Fatal(err)
	}
	if paidCalls.Load() != 0 || data.RpcEndpointCoolingDown(primaryNode) {
		t.Fatalf("valid older snapshot poisoned healthy public node: paid calls=%d, public head calls=%d, cooling=%t, reasons=%v", paidCalls.Load(), publicHeads.Load(), data.RpcEndpointCoolingDown(primaryNode), data.SnapshotRpcRoutingStats()[mdb.NetworkAptos].FallbackReasons)
	}
	// A later request genuinely regressing below the observed head must still
	// fall back; accepting the overlapping snapshot must not lower that head.
	currentHead.Store(110)
	if head, err := AptosIndexerHead(ctx); err != nil || head != 111 {
		t.Fatalf("regression did not use healthy fallback: head=%d err=%v", head, err)
	}
	if paidCalls.Load() != 1 || !data.RpcEndpointCoolingDown(primaryNode) {
		t.Fatal("real sequential regression was accepted")
	}
}

func TestSolRPCHealthProbesRespectSharedRequestBudget(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	isolateSolThrottle(t, 2)
	viper.Set("rpc_public_requests_per_second", 2)
	var mu sync.Mutex
	var requests []time.Time
	nodes := make([]mdb.RpcNode, 3)
	for i := range nodes {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requests = append(requests, time.Now())
			mu.Unlock()
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":100}`))
		}))
		t.Cleanup(server.Close)
		nodes[i] = mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposeGeneral}
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(node mdb.RpcNode) {
			defer wg.Done()
			<-start
			if err := ProbeRPCNode(node); err != nil {
				t.Error(err)
			}
		}(node)
	}
	// Include an ordinary scan request in the same burst. Probes and scans
	// must consume one shared budget rather than two independent budgets.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if _, err := solRetryClientWithURL(nodes[0].Url, "getBlockHeight", nil); err != nil {
			t.Error(err)
		}
	}()
	close(start)
	wg.Wait()
	sort.Slice(requests, func(i, j int) bool { return requests[i].Before(requests[j]) })
	if len(requests) != len(nodes)+1 {
		t.Fatalf("probe calls=%d", len(requests))
	}
	for i := 1; i < len(requests); i++ {
		if gap := requests[i].Sub(requests[i-1]); gap < 400*time.Millisecond {
			t.Fatalf("health probes bypassed 2 RPS global budget: gap=%s, three-probe span=%s", gap, requests[len(requests)-1].Sub(requests[0]))
		}
	}
}

func TestRPCProbeLocalBudgetTimeoutIsDeferred(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	isolateSolThrottle(t, 2)
	viper.Set("rpc_public_requests_per_second", 0.1)
	solRPCThrottle.next = time.Now().Add(time.Second)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"result":100}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	node := mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL}
	err := probeRPCNode(ctx, node)
	if !errors.Is(err, ErrRPCProbeDeferred) || calls.Load() != 0 {
		t.Fatalf("local budget exhaustion was reported as remote failure: err=%v calls=%d", err, calls.Load())
	}
	// Failing the shared-budget check must not pre-consume the endpoint's
	// ten-second token. Once the shared gate opens this request can go now.
	solRPCThrottle.mu.Lock()
	solRPCThrottle.next = time.Time{}
	solRPCThrottle.mu.Unlock()
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer retryCancel()
	if err := probeRPCNode(retryCtx, node); err != nil || calls.Load() != 1 {
		t.Fatalf("cancelled request consumed endpoint capacity: err=%v calls=%d", err, calls.Load())
	}
}

func TestAptosIndexerProbesShareQueryBudget(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	oldLimiter := aptosIndexerLimiter
	aptosIndexerLimiter = rate.NewLimiter(2, 1)
	t.Cleanup(func() { aptosIndexerLimiter = oldLimiter })
	var mu sync.Mutex
	var requests []time.Time
	var probes []mdb.RpcNode
	for i := 0; i < 3; i++ {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requests = append(requests, time.Now())
			mu.Unlock()
			_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":100}]}}`))
		}))
		t.Cleanup(server.Close)
		probes = append(probes, mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: server.URL})
	}
	viper.Set("aptos_indexer_url", probes[0].Url)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, node := range probes[1:] {
		wg.Add(1)
		go func(node mdb.RpcNode) {
			defer wg.Done()
			<-start
			if err := ProbeRPCNode(node); err != nil {
				t.Error(err)
			}
		}(node)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if _, err := AptosIndexerHead(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	close(start)
	wg.Wait()
	sort.Slice(requests, func(i, j int) bool { return requests[i].Before(requests[j]) })
	if len(requests) != 3 {
		t.Fatalf("request count=%d", len(requests))
	}
	for i := 1; i < len(requests); i++ {
		if gap := requests[i].Sub(requests[i-1]); gap < 400*time.Millisecond {
			t.Fatalf("indexer probe bypassed shared budget: gap=%s", gap)
		}
	}
}
