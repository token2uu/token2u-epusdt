package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

func TestAptosIndexerUsesAvailableAttemptBudget(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		public, paid, healthy, cooling int
		want                           []int32
	}{
		{"third_public", 3, 0, 2, -1, []int32{1, 1, 1}},
		{"reserve_paid", 3, 1, 3, -1, []int32{1, 1, 0, 1}},
		{"paid_cooling_uses_public", 3, 1, 2, 3, []int32{1, 1, 1, 0}},
		{"second_paid", 1, 2, 2, -1, []int32{1, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			data.ResetRpcEndpointPolicyForTest()
			t.Cleanup(data.ResetRpcEndpointPolicyForTest)
			data.ResetRpcFailoverForTest()
			t.Cleanup(data.ResetRpcFailoverForTest)
			oldLimiter := aptosIndexerLimiter
			aptosIndexerLimiter = rate.NewLimiter(rate.Inf, 1)
			t.Cleanup(func() { aptosIndexerLimiter = oldLimiter })
			calls := make([]atomic.Int32, tc.public+tc.paid)
			for i := range calls {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls[i].Add(1)
					if i != tc.healthy {
						w.WriteHeader(503)
						return
					}
					_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":110}]}}`))
				}))
				t.Cleanup(server.Close)
				purpose := mdb.RpcNodePurposeGeneral
				if i >= tc.public {
					purpose = mdb.RpcNodePurposePaidFallback
				}
				node := mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: server.URL, Purpose: purpose, Weight: 20 - i, Enabled: true, Status: mdb.RpcNodeStatusOk}
				if err := dao.Mdb.Create(&node).Error; err != nil {
					t.Fatal(err)
				}
				if i == tc.cooling {
					data.CooldownRpcEndpoint(node, "60")
				}
			}
			head, err := AptosIndexerHead(context.Background())
			if err != nil || head != 110 {
				t.Fatalf("head=%d err=%v", head, err)
			}
			for i := range calls {
				if calls[i].Load() != tc.want[i] {
					t.Fatalf("endpoint %d calls=%d want=%d", i, calls[i].Load(), tc.want[i])
				}
			}
		})
	}
}

func TestAptosIndexerSameURLDifferentKeyFallback(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcRoutingStatsForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcRoutingStatsForTest)
	var anonymous, authenticated atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			anonymous.Add(1)
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if r.Header.Get("Authorization") != "Bearer fallback-test-key" {
			t.Error("unexpected fallback authorization")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		authenticated.Add(1)
		_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":110}],"fungible_asset_activities":[]}}`))
	}))
	defer server.Close()
	viper.Set("aptos_indexer_url", server.URL)
	viper.Set("aptos_indexer_api_key", "")
	viper.Set("aptos_indexer_fallback_url", server.URL)
	viper.Set("aptos_indexer_fallback_api_key", "fallback-test-key")
	nodes, err := aptosIndexerNodes()
	if err != nil || len(nodes) != 2 || nodes[1].Purpose != mdb.RpcNodePurposePaidFallback {
		t.Fatalf("authenticated fallback missing: node count=%d err=%v", len(nodes), err)
	}
	head, err := AptosIndexerHead(context.Background())
	if err != nil || head != 110 {
		t.Fatalf("head=%d err=%v", head, err)
	}
	rows, err := AptosIndexerDeposits(context.Background(), "0xabc", AptosEventPosition{Version: 100}, head, 100)
	if err != nil || len(rows) != 0 {
		t.Fatalf("deposit query fallback failed: rows=%d err=%v", len(rows), err)
	}
	if anonymous.Load() != 1 || authenticated.Load() != 2 {
		t.Fatalf("cooldown crossed credentials: anonymous=%d authenticated=%d", anonymous.Load(), authenticated.Load())
	}
	// Identical URL and credentials still represent one endpoint.
	viper.Set("aptos_indexer_api_key", "fallback-test-key")
	nodes, err = aptosIndexerNodes()
	if err != nil || len(nodes) != 1 {
		t.Fatalf("identical endpoint not deduplicated: node count=%d err=%v", len(nodes), err)
	}
}

func TestAptosBootstrapTimeFilterSurvivesIndexerFallback(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	const since = "2026-10-12T01:00:00Z"
	var calls atomic.Int32
	for i, key := range []string{"aptos_indexer_url", "aptos_indexer_fallback_url"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			var req struct {
				Query     string
				Variables map[string]interface{}
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if !strings.Contains(req.Query, "transaction_timestamp: {_gte: $since}") || req.Variables["since"] != since || req.Variables["version"] != "-1" || req.Variables["head"] != "2000" {
				t.Error("fallback lost frozen scan boundary")
			}
			if i == 0 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":2000}],"fungible_asset_activities":[]}}`))
		}))
		t.Cleanup(server.Close)
		viper.Set(key, server.URL)
	}
	rows, err := AptosIndexerDepositsSince(context.Background(), "0xa", AptosEventPosition{Version: -1}, 2000, 100, since)
	if err != nil || len(rows) != 0 || calls.Load() != 2 {
		t.Fatalf("query failed: rows=%d calls=%d err=%v", len(rows), calls.Load(), err)
	}
}
