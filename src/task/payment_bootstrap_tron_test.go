package task

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestTronBootstrapRecoversPaymentAfterInitialOutage(t *testing.T) {
	for _, saved := range []bool{true, false} {
		name := "without-cursor"
		if saved {
			name = "with-cursor-control"
		}
		t.Run(name, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			resetPaidFallbackState(t)
			if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", mdb.NetworkTron).Update("min_confirmations", 1).Error; err != nil {
				t.Fatal(err)
			}
			owner, _ := HexToTronAddress(strings.Repeat("1", 40))
			ownerHex, _ := TronAddressToHex(owner)
			token := mdb.ChainToken{Network: mdb.NetworkTron, Symbol: "TRX", Decimals: 6, Enabled: true}
			if err := dao.Mdb.Create(&token).Error; err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			blocks := map[int64]map[string]interface{}{}
			for n := int64(1); n <= 4; n++ {
				txs := []interface{}{}
				if n == 2 {
					txs = append(txs, map[string]interface{}{"ret": []interface{}{map[string]string{"contractRet": "SUCCESS"}}, "raw_data": map[string]interface{}{"contract": []interface{}{map[string]interface{}{"type": "TransferContract", "parameter": map[string]interface{}{"value": map[string]interface{}{"owner_address": "41" + strings.Repeat("2", 40), "to_address": ownerHex, "amount": 3000000}}}}}})
				}
				b := map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": n, "timestamp": now.Add(time.Duration(n-4) * 3 * time.Second).UnixMilli()}}, "transactions": txs}
				sealTronTestBlock(t, b)
				blocks[n] = b
			}
			var up atomic.Bool
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !up.Load() {
					http.Error(w, "temporary outage", 503)
					return
				}
				n := int64(4)
				if r.URL.Path == "/wallet/getblockbynum" {
					var q struct {
						Num int64 `json:"num"`
					}
					_ = json.NewDecoder(r.Body).Decode(&q)
					n = q.Num
				}
				if n == 2 {
					reads.Add(1)
				}
				_ = json.NewEncoder(w).Encode(blocks[n])
			}))
			defer server.Close()
			node := mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposeGeneral, Enabled: true, Status: mdb.RpcNodeStatusOk}
			if err := dao.Mdb.Create(&node).Error; err != nil {
				t.Fatal(err)
			}
			if saved {
				if err := data.SavePaymentCursor("tron:blocks", "1"); err != nil {
					t.Fatal(err)
				}
			}
			if !saved {
				if err := NewScanner().Init(); err == nil {
					t.Fatal("expected startup outage")
				}
			}
			deadline := now.Add(time.Minute)
			order := &mdb.Orders{TradeId: "during-outage", OrderId: "during-outage", Network: mdb.NetworkTron, Token: "TRX", Amount: 21, ActualAmount: 3, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &deadline}
			if err := data.CreateExclusivePayment(order, token, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(order).Update("assigned_at_ms", now.Add(-9*time.Second).UnixMilli()).Error; err != nil {
				t.Fatal(err)
			}
			up.Store(true)
			data.ResetRpcFailoverForTest()
			data.ResetRpcRuntimeStatsForTest()
			data.ResetRpcEndpointPolicyForTest()
			scanner := NewScanner()
			if err := scanner.Init(); err != nil {
				t.Fatal(err)
			}
			scanner.poll()
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != mdb.StatusPaySuccess {
				t.Fatalf("valid new-version payment skipped: status=%d cursor=%d payment-block-reads=%d", got.Status, scanner.lastBlock, reads.Load())
			}
		})
	}
}

func TestTronBootstrapHistoryFailureDoesNotCommitCursor(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	base := time.Now().Add(-12 * time.Second).Truncate(time.Second)
	boundary := base.Add(2 * time.Second)
	if err := data.SavePaymentCursor("tron:bootstrap_time", boundary.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	blocks := map[int64]map[string]interface{}{}
	for n := int64(1); n <= 5; n++ {
		b := map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": n, "timestamp": base.Add(time.Duration(n) * time.Second).UnixMilli()}}, "transactions": []interface{}{}}
		sealTronTestBlock(t, b)
		blocks[n] = b
	}
	var fail atomic.Bool
	fail.Store(true)
	var calls atomic.Int32
	var head atomic.Int64
	head.Store(4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		n := head.Load()
		if r.URL.Path == "/wallet/getblockbynum" {
			var q struct {
				Num int64 `json:"num"`
			}
			_ = json.NewDecoder(r.Body).Decode(&q)
			n = q.Num
		}
		if n == 3 && fail.Load() {
			http.Error(w, "history unavailable", 503)
			return
		}
		_ = json.NewEncoder(w).Encode(blocks[n])
	}))
	defer server.Close()
	node := mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposeGeneral, Enabled: true, Status: mdb.RpcNodeStatusOk}
	if err := dao.Mdb.Create(&node).Error; err != nil {
		t.Fatal(err)
	}
	if err := NewScanner().Init(); err == nil {
		t.Fatal("expected history error")
	}
	stored, err := data.LoadPaymentCursor("tron:blocks")
	if err != nil || stored != "" {
		t.Fatalf("failed bootstrap committed %q: %v", stored, err)
	}
	fail.Store(false)
	head.Store(5)
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	scanner := NewScanner()
	if err := scanner.Init(); err != nil {
		t.Fatal(err)
	}
	if scanner.lastBlock != 1 {
		t.Fatalf("retry skipped boundary block: %d", scanner.lastBlock)
	}
	before := calls.Load()
	scanner = NewScanner()
	if err := scanner.Init(); err != nil || scanner.lastBlock != 1 || calls.Load() != before {
		t.Fatalf("saved cursor restart: %d calls=%d err=%v", scanner.lastBlock, calls.Load()-before, err)
	}
}
