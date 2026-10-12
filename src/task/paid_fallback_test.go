package task

import (
	"context"
	"encoding/json"
	"math"
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
	"github.com/GMWalletApp/epusdt/model/service"
)

func resetPaidFallbackState(t *testing.T) {
	t.Helper()
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	data.ResetRpcRoutingStatsForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcFailoverForTest)
	t.Cleanup(data.ResetRpcRoutingStatsForTest)
}

func TestTronPaidFallbackSettlesAndReturnsToPublic(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner, err := HexToTronAddress(strings.Repeat("1", 40))
	if err != nil {
		t.Fatal(err)
	}
	ownerHex, err := TronAddressToHex(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Create(&mdb.ChainToken{Network: mdb.NetworkTron, Symbol: "TRX", Decimals: 6, Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	order := &mdb.Orders{TradeId: "tron-fallback", OrderId: "tron-fallback", Network: mdb.NetworkTron, Token: "TRX", Amount: 21, ActualAmount: 3, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, mdb.ChainToken{Network: mdb.NetworkTron, Symbol: "TRX", Decimals: 6}, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	block := map[string]interface{}{"blockID": "block2", "block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": 2, "timestamp": time.Now().UnixMilli(), "txTrieRoot": strings.Repeat("1", 64)}}, "transactions": []interface{}{map[string]interface{}{"txID": "paid-tron-tx", "ret": []interface{}{map[string]string{"contractRet": "SUCCESS"}}, "raw_data": map[string]interface{}{"contract": []interface{}{map[string]interface{}{"type": "TransferContract", "parameter": map[string]interface{}{"value": map[string]interface{}{"owner_address": "41" + strings.Repeat("2", 40), "to_address": ownerHex, "amount": 3000000}}}}}}}}
	sealTronTestBlock(t, block)
	var healthy atomic.Bool
	var publicCalls, paidCalls atomic.Int32
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		publicCalls.Add(1)
		if !healthy.Load() {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(block)
	}))
	defer public.Close()
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { paidCalls.Add(1); _ = json.NewEncoder(w).Encode(block) }))
	defer paid.Close()
	for i, endpoint := range []string{public.URL, paid.URL} {
		purpose := mdb.RpcNodePurposeGeneral
		if i == 1 {
			purpose = mdb.RpcNodePurposePaidFallback
		}
		if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: endpoint, Purpose: purpose, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
			t.Fatal(err)
		}
	}
	scanner := NewScanner()
	scanner.lastBlock = 1
	scanner.poll()
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || scanner.lastBlock != 2 {
		t.Fatalf("fallback did not settle/catch up: order=%+v cursor=%d err=%v", got, scanner.lastBlock, err)
	}
	healthy.Store(true)
	data.ResetRpcEndpointPolicyForTest()
	scanner.poll()
	if publicCalls.Load() != 2 || paidCalls.Load() != 1 {
		t.Fatalf("paid endpoint remained sticky: public=%d paid=%d", publicCalls.Load(), paidCalls.Load())
	}
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if count != 1 {
		t.Fatalf("receipt replay: %d", count)
	}
}

func TestAptosPaidIndexerAndRPCFallbackSettleOnce(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	order := &mdb.Orders{TradeId: "aptos-paid-fallback", OrderId: "aptos-paid-fallback", Network: mdb.NetworkAptos, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, *asset, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
		t.Fatal(err)
	}
	tx := aptosDepositTransaction(t, owner, asset.ContractAddress, 105)
	var paidRPC, paidIndexer atomic.Int32
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer public.Close()
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transactions/by_version/105" {
			paidRPC.Add(1)
			_, _ = w.Write(tx)
			return
		}
		if r.URL.Path != "/graphql" {
			t.Errorf("unexpected request path: %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		paidIndexer.Add(1)
		var req struct {
			Query     string
			Variables map[string]interface{}
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		response := map[string]interface{}{"ledger_infos": []interface{}{map[string]int{"chain_id": 1}}, "processor_status": []interface{}{map[string]int64{"last_success_version": 110}}}
		if strings.Contains(req.Query, "PaymentDeposits") {
			if req.Variables["owner"] != owner {
				t.Error("fallback lost address")
			}
			switch req.Variables["version"] {
			case "100":
				response["fungible_asset_activities"] = []interface{}{indexedDeposit(owner, asset.ContractAddress, 105, 1, "3000000")}
			case "105":
				if req.Variables["event"] != "1" {
					t.Error("lost within-transaction cursor")
				}
				response["fungible_asset_activities"] = []interface{}{}
			default:
				t.Error("fallback lost cursor")
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": response})
	}))
	defer paid.Close()
	for _, typ := range []string{mdb.RpcNodeTypeHttp, mdb.RpcNodeTypeIndexer} {
		for i, endpoint := range []string{public.URL, paid.URL} {
			purpose := mdb.RpcNodePurposeGeneral
			if i == 1 {
				purpose = mdb.RpcNodePurposePaidFallback
			}
			if typ == mdb.RpcNodeTypeIndexer {
				endpoint += "/graphql"
			}
			if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkAptos, Type: typ, Url: endpoint, Purpose: purpose, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 2; i++ {
		if err := scanAptosDepositRound(context.Background(), serviceAptosDepositProvider{}, state, 1); err != nil {
			t.Fatal(err)
		}
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" {
		t.Fatalf("paid fallback failed: %+v %v", got, err)
	}
	if paidRPC.Load() != 1 || paidIndexer.Load() != 4 {
		t.Fatalf("unnecessary paid reads: rpc=%d indexer=%d", paidRPC.Load(), paidIndexer.Load())
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 110, Index: math.MaxInt64})
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if count != 1 {
		t.Fatalf("expected one receipt, got %d", count)
	}
}
