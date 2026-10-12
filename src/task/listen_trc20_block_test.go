package task

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestTronScannerStopsOnHistoricalBlockFetchError(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcFailoverForTest)

	node := &mdb.RpcNode{
		Network: mdb.NetworkTron,
		Type:    mdb.RpcNodeTypeHttp,
		Weight:  1,
		Enabled: true,
		Purpose: mdb.RpcNodePurposeGeneral,
		Status:  mdb.RpcNodeStatusOk,
	}
	if err := dao.Mdb.Create(node).Error; err != nil {
		t.Fatalf("seed rpc_node: %v", err)
	}

	head := map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": 3, "timestamp": 1000}}, "transactions": []interface{}{}}
	sealTronTestBlock(t, head)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/wallet/getnowblock":
			_ = json.NewEncoder(w).Encode(head)
		case "/wallet/getblockbynum":
			http.Error(w, "temporary", http.StatusBadGateway)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	node.Url = server.URL
	if err := dao.Mdb.Model(node).Update("url", server.URL).Error; err != nil {
		t.Fatal(err)
	}
	scanner := NewScanner()
	scanner.useRpcNode(node)
	scanner.lastBlock = 1

	scanner.poll()
	if scanner.lastBlock != 1 {
		t.Fatalf("lastBlock = %d, want 1 so failed block is retried", scanner.lastBlock)
	}
}

func TestTronRPCRecordsRuntimeStats(t *testing.T) {
	data.ResetRpcRuntimeStatsForTest()
	t.Cleanup(data.ResetRpcRuntimeStatsForTest)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/wallet/getnowblock":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"block_header": map[string]interface{}{
					"raw_data": map[string]interface{}{
						"number":     10,
						"timestamp":  int64(1000),
						"txTrieRoot": strings.Repeat("0", 64),
					},
				},
				"transactions": []interface{}{},
			})
		case "/wallet/getblockbynum":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"block_header": map[string]interface{}{
					"raw_data": map[string]interface{}{
						"number":     9,
						"timestamp":  int64(1000),
						"txTrieRoot": strings.Repeat("0", 64),
					},
				},
				"transactions": []interface{}{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if _, err := GetNowBlock(server.URL, ""); err != nil {
		t.Fatalf("GetNowBlock(): %v", err)
	}
	if _, err := GetBlockByNum(server.URL, "", 9); err != nil {
		t.Fatalf("GetBlockByNum(): %v", err)
	}

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary", http.StatusBadGateway)
	}))
	defer failing.Close()
	if _, err := GetBlockByNum(failing.URL, "", 8); err == nil {
		t.Fatal("GetBlockByNum() error = nil, want failure")
	}

	stats := data.SnapshotRpcRuntimeStats()
	tron := stats[mdb.NetworkTron]
	if tron.SuccessCount != 2 {
		t.Fatalf("tron success_count = %d, want 2", tron.SuccessCount)
	}
	if tron.FailureCount != 1 {
		t.Fatalf("tron failure_count = %d, want 1", tron.FailureCount)
	}
	if tron.LatestBlockHeight != 10 {
		t.Fatalf("tron latest_block_height = %d, want 10", tron.LatestBlockHeight)
	}
	if tron.LastSyncAt.IsZero() {
		t.Fatal("tron last_sync_at is zero")
	}
}
