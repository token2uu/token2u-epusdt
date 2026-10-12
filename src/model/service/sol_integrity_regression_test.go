package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/gagliardetto/solana-go"
)

func TestSolWrongDecimalsFallbackAndRecovery(t *testing.T) {
	for _, backupHealthy := range []bool{true, false} {
		name := "backup_available"
		if !backupHealthy {
			name = "all_failed_then_recovered"
		}
		t.Run(name, func(t *testing.T) {
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
			tx := l.transactions["B"].(map[string]interface{})
			valid, err := json.Marshal(tx)
			if err != nil {
				t.Fatal(err)
			}
			amount := tx["transaction"].(map[string]interface{})["message"].(map[string]interface{})["instructions"].([]interface{})[0].(map[string]interface{})["parsed"].(map[string]interface{})["info"].(map[string]interface{})["tokenAmount"].(map[string]interface{})
			amount["decimals"] = 9
			var calls atomic.Int32
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct{ Method string }
				_ = json.NewDecoder(r.Body).Decode(&request)
				if request.Method != "getTransaction" {
					_, _ = w.Write([]byte(`{"result":[]}`))
					return
				}
				calls.Add(1)
				if !backupHealthy {
					w.WriteHeader(503)
					return
				}
				_, _ = w.Write(append(append([]byte(`{"result":`), valid...), '}'))
			}))
			defer backup.Close()
			if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: backup.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
				t.Fatal(err)
			}
			err = ScanSolanaAddress(solCursorTestOwner)
			if !backupHealthy {
				if err == nil {
					t.Fatal("incorrect precision accepted")
				}
				assertSolCursor(t, l, "A")
				var receipts int64
				dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
				if receipts != 0 {
					t.Fatalf("precision error consumed receipt: %d", receipts)
				}
				amount["decimals"] = 6
				data.ResetRpcEndpointPolicyForTest()
				data.ResetRpcFailoverForTest()
				clearProcessedSignatures()
				err = ScanSolanaAddress(solCursorTestOwner)
			}
			if err != nil {
				t.Fatal(err)
			}
			assertSolCursor(t, l, "B")
			if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
				t.Fatal(err)
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			var receipts int64
			dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
			if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || receipts != 1 || calls.Load() != 1 {
				t.Fatalf("incorrect recovery: order=%+v receipts=%d backup=%d err=%v", got, receipts, calls.Load(), err)
			}
		})
	}
}

func TestSolWrongTransactionCannotSettle(t *testing.T) {
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
	requested := (solana.Signature{1}).String()
	actual := (solana.Signature{2}).String()
	l.add(requested, 2, "3000000")
	tx := l.transactions[requested].(map[string]interface{})
	tx["slot"] = 2
	tx["transaction"].(map[string]interface{})["signatures"] = []string{actual}
	var backupCalls atomic.Int32
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupCalls.Add(1); w.WriteHeader(503) }))
	defer backup.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: backup.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposePaidFallback}).Error; err != nil {
		t.Fatal(err)
	}
	if err := ScanSolanaAddress(solCursorTestOwner); err == nil {
		t.Fatal("mismatched transaction accepted")
	}
	assertSolCursor(t, l, "A")
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusWaitPay || count != 0 || backupCalls.Load() != 1 {
		t.Fatalf("invalid response had effects: order=%+v count=%d backup=%d err=%v", got, count, backupCalls.Load(), err)
	}
	tx["transaction"].(map[string]interface{})["signatures"] = []string{requested}
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	for i := 0; i < 2; i++ {
		if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
			t.Fatal(err)
		}
	}
	assertSolCursor(t, l, requested)
	got, err = data.GetOrderInfoByTradeId(order.TradeId)
	var receipt mdb.PaymentReceipt
	if e := dao.Mdb.First(&receipt).Error; e != nil {
		t.Fatal(e)
	}
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if err != nil || got.Status != mdb.StatusPaySuccess || receipt.TxHash != requested || count != 1 {
		t.Fatalf("recovery failed: order=%+v receipt=%+v count=%d err=%v", got, receipt, count, err)
	}
}

func TestSolInvalidSignatureListUsesFallback(t *testing.T) {
	var backupCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"truncated":true}}`))
	}))
	defer primary.Close()
	t.Cleanup(setupSolanaRPCNode(t, primary.URL))
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcFailoverForTest)
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":[]}`))
	}))
	defer backup.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: backup.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposePaidFallback}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := SolGetSignaturesForAddress(solCursorTestOwner, 100, "A", ""); err != nil {
			t.Fatal(err)
		}
	}
	if backupCalls.Load() != 3 {
		t.Fatalf("backup calls=%d", backupCalls.Load())
	}
}

func TestSolMissingBlockTimeRetriesAfterRecovery(t *testing.T) {
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
	tx := l.transactions["B"].(map[string]interface{})
	stamp := tx["blockTime"]
	tx["blockTime"] = nil
	if err := ScanSolanaAddress(solCursorTestOwner); err == nil {
		t.Fatal("missing incoming block time accepted")
	}
	assertSolCursor(t, l, "A")
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if count != 0 {
		t.Fatalf("missing time consumed receipt: %d", count)
	}
	tx["blockTime"] = stamp
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	for i := 0; i < 2; i++ {
		if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
			t.Fatal(err)
		}
	}
	assertSolCursor(t, l, "B")
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if err != nil || got.Status != mdb.StatusPaySuccess || count != 1 {
		t.Fatalf("recovery failed: order=%+v count=%d err=%v", got, count, err)
	}
}
