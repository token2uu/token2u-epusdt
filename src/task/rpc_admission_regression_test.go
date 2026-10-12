package task

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/spf13/viper"
)

func TestRPCEndpointBudgetDoesNotMarkHealthyPublicNodeDown(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	viper.Set("solana_rpc_requests_per_second", 2)
	viper.Set("rpc_public_requests_per_second", 0.1)
	var publicCalls, paidCalls atomic.Int32
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		publicCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":100}`))
	}))
	defer public.Close()
	node := mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: public.URL, Purpose: mdb.RpcNodePurposeGeneral, Enabled: true, Status: mdb.RpcNodeStatusOk}
	if err := dao.Mdb.Create(&node).Error; err != nil {
		t.Fatal(err)
	}
	checkedAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	if err := dao.Mdb.Model(&node).Update("last_checked_at", checkedAt).Error; err != nil {
		t.Fatal(err)
	}
	// A successful request consumes the endpoint's 10-second interval.
	if err := service.ProbeRPCNode(node); err != nil {
		t.Fatal(err)
	}
	RpcHealthJob{}.Run()
	got, err := data.GetRpcNodeByID(node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != mdb.RpcNodeStatusOk || got.LastCheckedAt.Timestamp() != checkedAt.Unix() || data.RpcEndpointCoolingDown(node) {
		t.Fatalf("local endpoint queue changed health: status=%s last_checked=%v cooling=%t", got.Status, got.LastCheckedAt, data.RpcEndpointCoolingDown(node))
	}
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paidCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
	}))
	defer paid.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: paid.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	// Ordinary requests may use available paid capacity, but repeated local
	// admission failures must not poison the public node's health/cooldown.
	for i := 0; i < 3; i++ {
		if _, err := service.SolRetryClient("getHealth", nil); err != nil {
			t.Fatal(err)
		}
	}
	if paidCalls.Load() != 3 || publicCalls.Load() != 1 || data.RpcEndpointCoolingDown(node) {
		t.Fatalf("local waiting poisoned failover: public=%d paid=%d cooling=%t", publicCalls.Load(), paidCalls.Load(), data.RpcEndpointCoolingDown(node))
	}
	viper.Set("rpc_public_requests_per_second", 1000)
	if _, err := service.SolRetryClient("getHealth", nil); err != nil {
		t.Fatal(err)
	}
	if publicCalls.Load() != 2 || paidCalls.Load() != 3 {
		t.Fatalf("available public capacity did not recover: public=%d paid=%d", publicCalls.Load(), paidCalls.Load())
	}
}
