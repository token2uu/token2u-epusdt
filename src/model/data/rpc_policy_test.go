package data

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func TestPublicPriorityReservesPaidFallbackAndRespectsCooldown(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	ResetRpcFailoverForTest()
	ResetRpcEndpointPolicyForTest()
	t.Cleanup(ResetRpcFailoverForTest)
	t.Cleanup(ResetRpcEndpointPolicyForTest)
	rows := []mdb.RpcNode{
		{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: "https://public-1.test", Weight: 10, Enabled: true, Status: mdb.RpcNodeStatusUnknown},
		{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: "https://public-2.test", Weight: 9, Enabled: true, Status: mdb.RpcNodeStatusUnknown},
		{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: "https://public-3.test", Weight: 8, Enabled: true, Status: mdb.RpcNodeStatusUnknown},
		{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: "https://paid.test", Weight: 1000, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposePaidFallback},
		{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: "https://manual.test", Weight: 9999, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposeManualVerify},
	}
	if err := dao.Mdb.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	node, err := SelectGeneralRpcNode(mdb.NetworkSolana, mdb.RpcNodeTypeHttp)
	if err != nil || node.ID != rows[0].ID {
		t.Fatalf("paid overrode public priority: %+v %v", node, err)
	}
	node, err = SelectGeneralRpcNode(mdb.NetworkSolana, mdb.RpcNodeTypeHttp, rows[0].ID, rows[1].ID)
	if err != nil || node.ID != rows[3].ID {
		t.Fatalf("third attempt did not reserve paid fallback: %+v %v", node, err)
	}
	now := time.Now()
	rpcEndpointPolicy.now = func() time.Time { return now }
	for _, row := range rows[:3] {
		CooldownRpcEndpoint(row, "60")
	}
	node, err = SelectGeneralRpcNode(mdb.NetworkSolana, mdb.RpcNodeTypeHttp)
	if err != nil || node.ID != rows[3].ID {
		t.Fatalf("cooling public did not use paid: %+v %v", node, err)
	}
	RecordRpcNodeSuccess(rows[0].ID) // In-flight success cannot erase a 429.
	if !RpcEndpointCoolingDown(rows[0]) {
		t.Fatal("success cleared endpoint cooldown")
	}
	now = now.Add(time.Minute)
	node, err = SelectGeneralRpcNode(mdb.NetworkSolana, mdb.RpcNodeTypeHttp)
	if err != nil || node.ID != rows[0].ID {
		t.Fatalf("did not return to public after recovery: %+v %v", node, err)
	}
}

func TestStalledHeadAndSoftPaidBudget(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	ResetRpcEndpointPolicyForTest()
	ResetRpcRoutingStatsForTest()
	t.Cleanup(ResetRpcEndpointPolicyForTest)
	t.Cleanup(ResetRpcRoutingStatsForTest)
	now := time.Now()
	rpcEndpointPolicy.now = func() time.Time { return now }
	node := mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: "https://indexer.test", Purpose: mdb.RpcNodePurposePaidFallback}
	if !ObserveRpcHead(node, 100, now) {
		t.Fatal("first head rejected")
	}
	now = now.Add(121 * time.Second)
	if ObserveRpcHead(node, 100, now) || ObserveRpcHead(node, 99, now) {
		t.Fatal("stalled/regressing head accepted")
	}
	if !ObserveRpcHead(node, 101, now) {
		t.Fatal("recovered head rejected")
	}
	viper.Set("rpc_paid_requests_per_minute", 2)
	RecordRpcRequest(node, "rate_limited", 200)
	RecordRpcRequest(node, "rate_limited", 200)
	if !PaidRpcBudgetExceeded(mdb.NetworkAptos) {
		t.Fatal("budget not tracked")
	}
	RecordRpcRequest(node, "history_unavailable", 200) // Core requests remain allowed.
	stats := SnapshotRpcRoutingStats()[mdb.NetworkAptos]
	if stats.PaidRequests != 3 || stats.FallbackReasons["rate_limited"] != 2 || !stats.PaidBudgetExceeded {
		t.Fatalf("incorrect routing stats: %+v", stats)
	}
	stats.FallbackReasons["rate_limited"] = 999
	if SnapshotRpcRoutingStats()[mdb.NetworkAptos].FallbackReasons["rate_limited"] != 2 {
		t.Fatal("snapshot mutated counters")
	}
}

func TestHeadOverlapDoesNotRewindOrRefreshProgress(t *testing.T) {
	ResetRpcEndpointPolicyForTest()
	t.Cleanup(ResetRpcEndpointPolicyForTest)
	now := time.Now()
	rpcEndpointPolicy.now = func() time.Time { return now }
	node := mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: "https://overlap.test"}
	olderStart := now
	now = now.Add(time.Second)
	if !ObserveRpcHead(node, 111, now) {
		t.Fatal("new head rejected")
	}
	now = now.Add(time.Second)
	if !ObserveRpcHead(node, 110, olderStart) {
		t.Fatal("overlapping older snapshot rejected")
	}
	if ObserveRpcHead(node, 110, now) {
		t.Fatal("old response rewound head and hid a later regression")
	}
	now = now.Add(120 * time.Second)
	if ObserveRpcHead(node, 110, olderStart) || ObserveRpcHead(node, 111, now) {
		t.Fatal("old response refreshed or bypassed stalled-head detection")
	}
}
