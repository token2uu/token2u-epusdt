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

func TestTronNearExpiryReviewUsesPaidAndReturnsToPublic(t *testing.T) {
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
	token := mdb.ChainToken{Network: mdb.NetworkTron, Symbol: "TRX", Decimals: 6, Enabled: true}
	if err := dao.Mdb.Create(&token).Error; err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Minute)
	order := &mdb.Orders{TradeId: "near-tron", OrderId: "near-tron", Network: mdb.NetworkTron, Token: "TRX", Amount: 21, ActualAmount: 3, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, token, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli())
	block := map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": 2, "timestamp": time.Now().UnixMilli()}}, "transactions": []interface{}{map[string]interface{}{"ret": []interface{}{map[string]string{"contractRet": "SUCCESS"}}, "raw_data": map[string]interface{}{"contract": []interface{}{map[string]interface{}{"type": "TransferContract", "parameter": map[string]interface{}{"value": map[string]interface{}{"owner_address": "41" + strings.Repeat("2", 40), "to_address": ownerHex, "amount": 3000000}}}}}}}}
	sealTronTestBlock(t, block)
	var publicCalls, paidCalls atomic.Int32
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { publicCalls.Add(1); _ = json.NewEncoder(w).Encode(block) }))
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
	got, _ := data.GetOrderInfoByTradeId(order.TradeId)
	if got.Status != mdb.StatusPaySuccess || publicCalls.Load() != 0 || paidCalls.Load() != 1 || scanner.lastBlock != 2 {
		t.Fatalf("near-expiry review failed: order=%+v public=%d paid=%d cursor=%d", got, publicCalls.Load(), paidCalls.Load(), scanner.lastBlock)
	}
	scanner.poll()
	if publicCalls.Load() != 1 || paidCalls.Load() != 1 {
		t.Fatal("paid priority remained after settlement")
	}
}

func TestAptosNearExpiryJobUsesPaidIndexerAndPaidEvidence(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	asset := state.tokens[0]
	order := seedAptosRegressionOrder(t, owner, asset)
	dao.Mdb.Model(order).Update("payment_expires_at", time.Now().Add(time.Minute))
	transaction := aptosDepositTransaction(t, owner, asset.ContractAddress, 105)
	var publicCalls, paidRPC, paidIndexer atomic.Int32
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		publicCalls.Add(1)
		http.Error(w, "public should not be used by deadline review", 500)
	}))
	defer public.Close()
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transactions/by_version/105" {
			paidRPC.Add(1)
			_, _ = w.Write(transaction)
			return
		}
		paidIndexer.Add(1)
		var req struct {
			Query     string
			Variables map[string]interface{}
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		response := map[string]interface{}{"ledger_infos": []interface{}{map[string]int{"chain_id": 1}}, "processor_status": []interface{}{map[string]int64{"last_success_version": 110}}}
		if strings.Contains(req.Query, "PaymentDeposits") {
			response["fungible_asset_activities"] = []interface{}{}
			if req.Variables["version"] == "-1" {
				response["fungible_asset_activities"] = []interface{}{indexedDeposit(owner, asset.ContractAddress, 105, 1, "3000000")}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": response})
	}))
	defer paid.Close()
	for _, kind := range []string{mdb.RpcNodeTypeHttp, mdb.RpcNodeTypeIndexer} {
		for i, endpoint := range []string{public.URL, paid.URL} {
			purpose := mdb.RpcNodePurposeGeneral
			if i == 1 {
				purpose = mdb.RpcNodePurposePaidFallback
			}
			if kind == mdb.RpcNodeTypeIndexer {
				endpoint += "/graphql"
			}
			if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkAptos, Type: kind, Url: endpoint, Purpose: purpose, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	job := &PaymentReviewJob{Network: mdb.NetworkAptos}
	job.Run()
	job.Run()
	assertAptosRegressionOrderSettled(t, order)
	if publicCalls.Load() != 0 || paidRPC.Load() != 1 || paidIndexer.Load() != 3 {
		t.Fatalf("incorrect review requests public=%d rpc=%d indexer=%d", publicCalls.Load(), paidRPC.Load(), paidIndexer.Load())
	}
	if cursor, err := data.LoadPaymentCursor("aptos:deposits:" + owner); err != nil || cursor != "" {
		t.Fatalf("review changed ordinary cursor: %s %v", cursor, err)
	}
}
