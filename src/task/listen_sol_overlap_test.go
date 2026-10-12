package task

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
)

func TestListenSolJobSkipsOverlapAndCatchesUpNextRun(t *testing.T) {
	resetSolanaSchedule(t)
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcRuntimeStatsForTest()
	t.Cleanup(data.ResetRpcRuntimeStatsForTest)
	const owner = "2uFTf9TZ8gd7Kg6hkb79TxfaeNpaAgpJ8uVHguv2Yweu"
	// The scanner's signature cache is process-wide, including repeated tests.
	signature := "overlap-payment-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var signatureCalls, heightCalls atomic.Int32
	var paymentAvailable atomic.Bool
	blockTime := time.Now().Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var result interface{}
		switch req.Method {
		case "getSignaturesForAddress":
			available := paymentAvailable.Load()
			if signatureCalls.Add(1) == 1 {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			var opts struct{ Until, Before string }
			if len(req.Params) != 2 || json.Unmarshal(req.Params[1], &opts) != nil {
				http.Error(w, "invalid signature query", http.StatusBadRequest)
				return
			}
			rows := []map[string]interface{}{}
			if available && opts.Until != signature && opts.Before != signature {
				rows = append(rows, map[string]interface{}{"signature": signature, "slot": 2, "err": nil, "blockTime": blockTime})
			}
			result = rows
		case "getTransaction":
			result = map[string]interface{}{
				"blockTime": blockTime, "meta": map[string]interface{}{"err": nil},
				"transaction": map[string]interface{}{"signatures": []string{signature}, "message": map[string]interface{}{"instructions": []interface{}{
					map[string]interface{}{"programId": service.SystemProgramID, "parsed": map[string]interface{}{
						"type": "transfer", "info": map[string]interface{}{"source": "sender", "destination": owner, "lamports": 3_000_000_000},
					}},
				}}},
			}
		case "getBlockHeight":
			heightCalls.Add(1)
			result = 123
		default:
			http.Error(w, "unexpected RPC", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	t.Cleanup(server.Close)
	var jobs sync.WaitGroup
	t.Cleanup(func() { unblock(); jobs.Wait() })
	startJob := func() <-chan struct{} {
		done := make(chan struct{})
		jobs.Add(1)
		go func() { defer jobs.Done(); defer close(done); (ListenSolJob{}).Run() }()
		return done
	}
	wait := func(done <-chan struct{}, message string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal(message)
		}
	}
	for _, row := range []interface{}{
		&mdb.RpcNode{Network: mdb.NetworkSolana, Url: server.URL, Type: mdb.RpcNodeTypeHttp, Weight: 1, Enabled: true, Purpose: mdb.RpcNodePurposeGeneral, Status: mdb.RpcNodeStatusOk},
		&mdb.WalletAddress{Network: mdb.NetworkSolana, Address: owner, Status: mdb.TokenStatusEnable},
		&mdb.ChainToken{Network: mdb.NetworkSolana, Symbol: "SOL", Decimals: 9, Enabled: true},
	} {
		if err := dao.Mdb.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	expires := time.Now().Add(10 * time.Minute)
	order := &mdb.Orders{TradeId: "overlap-order", OrderId: "overlap-order", Network: mdb.NetworkSolana, Token: "SOL",
		Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, mdb.ChainToken{Decimals: 9}, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	cursorKey := "solana:signatures:" + owner
	if err := data.SavePaymentCursor(cursorKey, "previous-payment"); err != nil {
		t.Fatal(err)
	}

	first := startJob()
	wait(entered, "first scan never reached RPC")
	paymentAvailable.Store(true) // Arrives after the in-flight RPC took its snapshot.
	skipped := make([]<-chan struct{}, 20)
	for i := range skipped {
		skipped[i] = startJob()
	}
	for _, done := range skipped {
		wait(done, "overlapping scan queued instead of returning")
	}
	if signatureCalls.Load() != 1 || heightCalls.Load() != 0 {
		t.Fatalf("skipped jobs issued RPC calls: signatures=%d heights=%d", signatureCalls.Load(), heightCalls.Load())
	}
	if cursor, err := data.LoadPaymentCursor(cursorKey); err != nil || cursor != "previous-payment" {
		t.Fatalf("skipped run changed cursor: %q %v", cursor, err)
	}
	unblock()
	wait(first, "first scan did not complete")
	if signatureCalls.Load() != 1 || heightCalls.Load() != 1 {
		t.Fatal("unexpected RPC calls after first scan")
	}

	wait(startJob(), "next scan failed to run after the lock was released")
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || got.PaidAmount != 21 || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("payment during skipped ticks was not settled: %+v %v", got, err)
	}
	if cursor, err := data.LoadPaymentCursor(cursorKey); err != nil || cursor != signature {
		t.Fatalf("cursor not advanced after catchup: %q %v", cursor, err)
	}
	wait(startJob(), "subsequent scan failed")
	var receipts int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != 1 {
		t.Fatalf("duplicate receipt: %d %v", receipts, err)
	}
}
