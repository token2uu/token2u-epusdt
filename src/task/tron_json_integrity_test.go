package task

import (
	"bytes"
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

func tronJSONPaymentFixture(t *testing.T) ([]byte, *mdb.Orders) {
	t.Helper()
	owner, err := HexToTronAddress(strings.Repeat("1", 40))
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	order := &mdb.Orders{TradeId: "json-integrity", OrderId: "json-integrity", Network: mdb.NetworkTron, Token: "TRX", Amount: 21, ActualAmount: 3, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, mdb.ChainToken{Network: mdb.NetworkTron, Symbol: "TRX", Decimals: 6}, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	var block map[string]interface{}
	if err := json.Unmarshal([]byte(`{"block_header":{"raw_data":{"number":2,"timestamp":1}},"transactions":[{"ret":[{"contractRet":"SUCCESS"}],"raw_data":{"contract":[{"type":"TransferContract","parameter":{"value":{"owner_address":"412222222222222222222222222222222222222222","to_address":"411111111111111111111111111111111111111111","amount":3000000}}}]}}]}`), &block); err != nil {
		t.Fatal(err)
	}
	block["block_header"].(map[string]interface{})["raw_data"].(map[string]interface{})["timestamp"] = time.Now().UnixMilli()
	sealTronTestBlock(t, block)
	raw, err := json.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	return raw, order
}

func TestTronAmbiguousJSONCannotChangePayment(t *testing.T) {
	for _, tc := range []struct {
		name, addition string
		backupCalls    int32
	}{
		{"duplicate_amount", `,"amount":30000000`, 1},
		{"escaped_duplicate_amount", `,"am\u006funt":30000000`, 1},
		{"case_alias_ignored", `,"Amount":30000000`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			resetPaidFallbackState(t)
			raw, order := tronJSONPaymentFixture(t)
			bad := bytes.Replace(raw, []byte(`"amount":3000000`), []byte(`"amount":3000000`+tc.addition), 1)
			var backupCalls atomic.Int32
			for i := 0; i < 2; i++ {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if i == 0 {
						_, _ = w.Write(bad)
						return
					}
					backupCalls.Add(1)
					_, _ = w.Write(raw)
				}))
				t.Cleanup(server.Close)
				purpose := mdb.RpcNodePurposeGeneral
				if i == 1 {
					purpose = mdb.RpcNodePurposePaidFallback
				}
				if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: purpose, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
					t.Fatal(err)
				}
			}
			scanner := NewScanner()
			scanner.lastBlock = 1
			scanner.poll()
			scanner.poll()
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			var receipts int64
			dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
			// A duplicate-key primary remains cooled down during the second poll.
			wantCalls := tc.backupCalls * 2
			if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || receipts != 1 || backupCalls.Load() != wantCalls {
				t.Fatalf("order=%+v receipts=%d backup=%d err=%v", got, receipts, backupCalls.Load(), err)
			}
		})
	}
}

func TestTronAmbiguousJSONRetainsCursorUntilRecovery(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	raw, order := tronJSONPaymentFixture(t)
	bad := bytes.Replace(raw, []byte(`"amount":3000000`), []byte(`"amount":3000000,"amount":30000000`), 1)
	var healthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			_, _ = w.Write(raw)
		} else {
			_, _ = w.Write(bad)
		}
	}))
	defer server.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposeGeneral, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.SavePaymentCursor("tron:blocks", "1"); err != nil {
		t.Fatal(err)
	}
	scanner := NewScanner()
	scanner.lastBlock = 1
	scanner.poll()
	stored, err := data.LoadPaymentCursor("tron:blocks")
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if err != nil || stored != "1" || scanner.lastBlock != 1 || count != 0 {
		t.Fatalf("ambiguous block had effects: cursor=%s count=%d err=%v", stored, count, err)
	}
	healthy.Store(true)
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	scanner.poll()
	scanner.poll()
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || count != 1 {
		t.Fatalf("recovery failed: order=%+v receipts=%d err=%v", got, count, err)
	}
}

func TestTronUppercaseTransactionAliasCannotInjectPayment(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	payment, order := tronJSONPaymentFixture(t)
	empty := map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": 2, "timestamp": time.Now().UnixMilli()}}}
	sealTronTestBlock(t, empty)
	var transaction map[string]json.RawMessage
	if err := json.Unmarshal(payment, &transaction); err != nil {
		t.Fatal(err)
	}
	empty["Transactions"] = transaction["transactions"]
	raw, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(raw) }))
	defer server.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposeGeneral, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	scanner := NewScanner()
	scanner.lastBlock = 1
	scanner.poll()
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if err != nil || scanner.lastBlock != 2 || got.Status != mdb.StatusWaitPay || count != 0 {
		t.Fatalf("uncommitted alias settled: order=%+v cursor=%d receipts=%d err=%v", got, scanner.lastBlock, count, err)
	}
}
