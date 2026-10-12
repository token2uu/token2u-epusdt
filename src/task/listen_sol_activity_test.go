package task

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func resetSolanaSchedule(t *testing.T) *solanaScanSchedule {
	t.Helper()
	previous := solanaSchedule
	solanaSchedule = newSolanaScanSchedule()
	t.Cleanup(func() { solanaSchedule = previous })
	return solanaSchedule
}

func TestSolanaActivityCadenceAndRestart(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	schedule := resetSolanaSchedule(t)
	now := time.Now()
	schedule.now = func() time.Time { return now }
	viper.Set("solana_idle_scan_interval_seconds", 300)
	viper.Set("solana_recent_activity_seconds", 600)
	viper.Set("solana_rpc_requests_per_second", 1000)
	var mu sync.Mutex
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		key := req.Method
		var result interface{} = 100
		if req.Method == "getSignaturesForAddress" {
			var owner string
			_ = json.Unmarshal(req.Params[0], &owner)
			key = owner
			result = []interface{}{}
		}
		mu.Lock()
		calls[key]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	t.Cleanup(server.Close)
	for _, row := range []interface{}{
		&mdb.RpcNode{Network: mdb.NetworkSolana, Url: server.URL, Type: mdb.RpcNodeTypeHttp, Weight: 1, Enabled: true, Status: mdb.RpcNodeStatusOk},
		&mdb.WalletAddress{Network: mdb.NetworkSolana, Address: "active-owner", Status: mdb.TokenStatusEnable},
		&mdb.WalletAddress{Network: mdb.NetworkSolana, Address: "idle-owner", Status: mdb.TokenStatusEnable},
		&mdb.ChainToken{Network: mdb.NetworkSolana, Symbol: "SOL", Decimals: 9, Enabled: true},
	} {
		if err := dao.Mdb.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	assertCalls := func(active, idle, height int) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		if calls["active-owner"] != active || calls["idle-owner"] != idle || calls["getBlockHeight"] != height {
			t.Fatalf("unexpected RPC counts: %v", calls)
		}
	}
	ListenSolJob{}.Run() // First discovery still scans every enabled address.
	assertCalls(1, 1, 1)
	now = now.Add(15 * time.Second)
	ListenSolJob{}.Run()
	assertCalls(1, 1, 1)
	expires := now.Add(time.Hour)
	order := &mdb.Orders{TradeId: "activity", OrderId: "activity", Network: mdb.NetworkSolana, ReceiveAddress: "active-owner", Status: mdb.StatusWaitPay, PaymentExpiresAt: &expires}
	if err := dao.Mdb.Create(order).Error; err != nil {
		t.Fatal(err)
	}
	ListenSolJob{}.Run() // A newly assigned order wakes the address immediately.
	assertCalls(2, 1, 1)
	now = now.Add(15 * time.Second)
	ListenSolJob{}.Run()
	assertCalls(3, 1, 1)
	if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"status": mdb.StatusPaySuccess, "updated_at": now}).Error; err != nil {
		t.Fatal(err)
	}
	now = now.Add(60 * time.Second)
	ListenSolJob{}.Run() // Recently paid addresses stay responsive.
	assertCalls(4, 1, 2)
	now = now.Add(10 * time.Minute)
	ListenSolJob{}.Run() // Expired activity window returns both to the idle cadence.
	assertCalls(5, 2, 3)
	now = now.Add(15 * time.Second)
	ListenSolJob{}.Run()
	assertCalls(5, 2, 3)
	now = now.Add(285 * time.Second)
	ListenSolJob{}.Run() // Cold addresses continue to poll for delayed transfers.
	assertCalls(6, 3, 4)
	solanaSchedule = newSolanaScanSchedule()
	solanaSchedule.now = func() time.Time { return now }
	ListenSolJob{}.Run()
	assertCalls(7, 4, 5)
	// A soft paid budget defers cold polling, but never a live payment.
	data.ResetRpcRoutingStatsForTest()
	t.Cleanup(data.ResetRpcRoutingStatsForTest)
	viper.Set("rpc_paid_requests_per_minute", 1)
	data.RecordRpcRequest(mdb.RpcNode{Network: mdb.NetworkSolana, Purpose: mdb.RpcNodePurposePaidFallback}, "test", 200)
	now = now.Add(5 * time.Minute)
	ListenSolJob{}.Run()
	assertCalls(7, 4, 5)
	if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"status": mdb.StatusWaitPay, "payment_expires_at": now.Add(time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	ListenSolJob{}.Run()
	assertCalls(8, 4, 6)
}
