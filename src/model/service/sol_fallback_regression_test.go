package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestSolPartialResponseUsesPaidFallback(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcFailoverForTest)
	l.add("A", 1, "")
	if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
		t.Fatal(err)
	}
	order := solCursorOrder(t)
	l.add("B", 2, "3000000")
	full := l.transactions["B"]
	var calls atomic.Int32
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Method string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		var result interface{} = []interface{}{}
		if req.Method == "getTransaction" {
			calls.Add(1)
			result = full
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "result": result})
	}))
	defer backup.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: backup.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposePaidFallback}).Error; err != nil {
		t.Fatal(err)
	}
	l.transactions["B"] = map[string]interface{}{"meta": map[string]interface{}{"err": nil}}
	for i := 0; i < 3; i++ {
		if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
			t.Fatal(err)
		}
		assertSolCursor(t, l, "B")
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || calls.Load() != 1 {
		t.Fatalf("unexpected result status=%v backup=%d err=%v", got.Status, calls.Load(), err)
	}
	var receipts int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
	if receipts != 1 {
		t.Fatalf("duplicate receipts: %d", receipts)
	}
}

func TestSolSameURLAuthFallbackAfter429(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	isolateSolThrottle(t, 1000)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcFailoverForTest)
	var public, paid atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			public.Add(1)
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(429)
			return
		}
		paid.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
	}))
	defer server.Close()
	for _, node := range []mdb.RpcNode{
		{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposeGeneral},
		{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, ApiKey: "review-key", Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposePaidFallback},
	} {
		if err := dao.Mdb.Create(&node).Error; err != nil {
			t.Fatal(err)
		}
	}
	_, err := SolRetryClient("getHealth", nil)
	if err != nil || public.Load() != 1 || paid.Load() != 1 {
		t.Fatalf("unexpected err=%v public=%d paid=%d", err, public.Load(), paid.Load())
	}
	if _, err := SolRetryClient("getHealth", nil); err != nil {
		t.Fatal(err)
	}
	if public.Load() != 1 || paid.Load() != 2 {
		t.Fatal("credential cooldown did not stay isolated")
	}
}
