package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func TestSolPublicFirstPaidFallbackAndRecovery(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	data.ResetRpcRoutingStatsForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcFailoverForTest)
	t.Cleanup(data.ResetRpcRoutingStatsForTest)
	isolateSolThrottle(t, 1000)
	var fail atomic.Bool
	var calls [4]atomic.Int32
	servers := make([]*httptest.Server, 4)
	for i := range servers {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls[i].Add(1)
			if i == 0 && !fail.Load() {
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
				return
			}
			if i < 3 {
				if i == 1 {
					w.Header().Set("Retry-After", "60")
					w.WriteHeader(429)
				} else {
					w.WriteHeader(503)
				}
				return
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
		}))
		t.Cleanup(servers[i].Close)
		purpose, weight := mdb.RpcNodePurposeGeneral, 10-i
		if i == 3 {
			purpose = mdb.RpcNodePurposePaidFallback
			weight = 999
		}
		if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: servers[i].URL, Purpose: purpose, Weight: weight, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := SolRetryClient("getHealth", nil); err != nil {
		t.Fatal(err)
	}
	if calls[3].Load() != 0 {
		t.Fatal("healthy public node consumed paid requests")
	}
	fail.Store(true)
	if _, err := SolRetryClient("getHealth", nil); err != nil {
		t.Fatal(err)
	}
	if calls[0].Load() != 2 || calls[1].Load() != 1 || calls[2].Load() != 0 || calls[3].Load() != 1 {
		t.Fatalf("did not reserve third attempt for paid: %v/%v/%v/%v", calls[0].Load(), calls[1].Load(), calls[2].Load(), calls[3].Load())
	}
	stats := data.SnapshotRpcRoutingStats()[mdb.NetworkSolana]
	if stats.PaidRequests != 1 || stats.FallbackReasons["rate_limited"] == 0 {
		t.Fatalf("missing paid/fallback counters: %+v", stats)
	}
	data.ResetRpcEndpointPolicyForTest()
	fail.Store(false)
	if _, err := SolRetryClient("getHealth", nil); err != nil {
		t.Fatal(err)
	}
	if calls[3].Load() != 1 {
		t.Fatal("did not return to recovered public node")
	}
}

func TestAptosIndexerFallsBackOnPartialDataAndKeepsCredentialsSeparate(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcFailoverForTest)
	var publicCalls, paidCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		publicCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer public-key" {
			t.Error("wrong public credentials")
		}
		_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":110}]},"errors":[{"message":"partial response"}]}`))
	}))
	defer primary.Close()
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paidCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer paid-key" {
			t.Error("credentials leaked across endpoints")
		}
		var req struct{ Variables map[string]interface{} }
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Variables["version"] != "100" || req.Variables["head"] != "110" {
			t.Error("fallback changed query window")
		}
		_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":110}],"fungible_asset_activities":[]}}`))
	}))
	defer paid.Close()
	viper.Set("aptos_indexer_url", primary.URL)
	viper.Set("aptos_indexer_api_key", "public-key")
	viper.Set("aptos_indexer_fallback_url", paid.URL)
	viper.Set("aptos_indexer_fallback_api_key", "paid-key")
	for i := 0; i < 2; i++ {
		rows, err := AptosIndexerDeposits(context.Background(), "0xa", AptosEventPosition{Version: 100}, 110, 100)
		if err != nil || rows == nil || len(rows) != 0 {
			t.Fatalf("fallback failed: %v %v", rows, err)
		}
	}
	if publicCalls.Load() != 1 || paidCalls.Load() != 2 {
		t.Fatalf("partial result was accepted or public cooldown lost: public=%d paid=%d", publicCalls.Load(), paidCalls.Load())
	}
}
