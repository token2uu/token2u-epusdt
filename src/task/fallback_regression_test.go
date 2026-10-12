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

func TestTronIncompleteBlockUsesPaidFallback(t *testing.T) {
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
	var full map[string]interface{}
	if err := json.Unmarshal([]byte(`{"blockID":"block2","block_header":{"raw_data":{"number":2,"timestamp":1800000000000}},"transactions":[{"txID":"incoming","ret":[{"contractRet":"SUCCESS"}],"raw_data":{"contract":[{"type":"TransferContract","parameter":{"value":{"owner_address":"412222222222222222222222222222222222222222","amount":3000000}}}]}}]}`), &full); err != nil {
		t.Fatal(err)
	}
	full["block_header"].(map[string]interface{})["raw_data"].(map[string]interface{})["txTrieRoot"] = strings.Repeat("1", 64)
	full["transactions"].([]interface{})[0].(map[string]interface{})["raw_data"].(map[string]interface{})["contract"].([]interface{})[0].(map[string]interface{})["parameter"].(map[string]interface{})["value"].(map[string]interface{})["to_address"] = ownerHex
	partial := map[string]interface{}{"blockID": full["blockID"], "block_header": full["block_header"]}
	var backupCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(partial) }))
	defer primary.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupCalls.Add(1); _ = json.NewEncoder(w).Encode(full) }))
	defer backup.Close()
	for i, u := range []string{primary.URL, backup.URL} {
		purpose := mdb.RpcNodePurposeGeneral
		if i == 1 {
			purpose = mdb.RpcNodePurposePaidFallback
		}
		if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: u, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: purpose}).Error; err != nil {
			t.Fatal(err)
		}
	}
	expires := time.Now().Add(time.Hour)
	order := &mdb.Orders{TradeId: "tron-incomplete", OrderId: "tron-incomplete", Network: mdb.NetworkTron, Token: "TRX", Amount: 21, ActualAmount: 3, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, mdb.ChainToken{Network: mdb.NetworkTron, Symbol: "TRX", Decimals: 6}, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	full["block_header"].(map[string]interface{})["raw_data"].(map[string]interface{})["timestamp"] = time.Now().UnixMilli()
	sealTronTestBlock(t, full)
	partial["blockID"] = full["blockID"]
	scanner := NewScanner()
	scanner.lastBlock = 1
	scanner.poll()
	stored, err := data.LoadPaymentCursor("tron:blocks")
	if err != nil || stored != "2" || backupCalls.Load() != 1 {
		t.Fatalf("stored=%s backup=%d err=%v", stored, backupCalls.Load(), err)
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess {
		t.Fatalf("did not settle via backup: %+v %v", got, err)
	}
	scanner.poll()
	var receipts int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
	if receipts != 1 {
		t.Fatalf("receipts=%d", receipts)
	}
}

func TestTronInvalidCandidatesRetainCursor(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	var calls atomic.Int32
	for _, purpose := range []string{mdb.RpcNodePurposeGeneral, mdb.RpcNodePurposePaidFallback} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			_, _ = w.Write([]byte(`{"blockID":"block2","block_header":{"raw_data":{"number":2,"timestamp":1000,"txTrieRoot":"` + strings.Repeat("1", 64) + `"}}}`))
		}))
		t.Cleanup(server.Close)
		if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: purpose}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := data.SavePaymentCursor("tron:blocks", "1"); err != nil {
		t.Fatal(err)
	}
	scanner := NewScanner()
	scanner.lastBlock = 1
	scanner.poll()
	stored, err := data.LoadPaymentCursor("tron:blocks")
	if err != nil || stored != "1" || scanner.lastBlock != 1 || calls.Load() != 2 {
		t.Fatalf("invalid candidates advanced cursor: persisted=%s memory=%d calls=%d err=%v", stored, scanner.lastBlock, calls.Load(), err)
	}
}

type failingHeadReviewProvider struct {
	fakeAptosDepositProvider
	fail bool
}

func (p *failingHeadReviewProvider) Head(ctx context.Context) (int64, error) {
	if p.fail {
		return 0, context.DeadlineExceeded
	}
	return p.fakeAptosDepositProvider.Head(ctx)
}
func TestAptosFirstHeadFailurePreservesPayment(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	order := seedAptosRegressionOrder(t, owner, state.tokens[0])
	asset := state.tokens[0].ContractAddress
	p := &failingHeadReviewProvider{fakeAptosDepositProvider: fakeAptosDepositProvider{head: 1000, rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset, 1050, 1, "3000000")}}, transactions: map[int64][]byte{1050: aptosDepositTransaction(t, owner, asset, 1050)}}, fail: true}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err == nil {
		t.Fatal("expected head failure")
	}
	start, err := data.LoadPaymentCursor("aptos:deposits:" + owner + ":start_time")
	if err != nil || start == "" {
		t.Fatalf("initial outage did not persist a start boundary: %q %v", start, err)
	}
	p.fail = false
	p.head = 2000
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 2000, Index: math.MaxInt64})
	if after, _ := data.LoadPaymentCursor("aptos:deposits:" + owner + ":start_time"); after != start {
		t.Fatal("recovery moved the initial time boundary")
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || p.transactionCalls != 1 {
		t.Fatalf("order status=%v calls=%d err=%v", got.Status, p.transactionCalls, err)
	}
	var receipts int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
	if receipts != 1 {
		t.Fatalf("receipts=%d", receipts)
	}
}

func TestAptosCorruptBootstrapDoesNotBlockOtherAddress(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owners := seedAptosScannerChain(t, "0xa", "0xb")
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	order := seedAptosRegressionOrder(t, owners[1], state.tokens[0])
	if err := data.SavePaymentCursor("aptos:deposits:"+owners[0], "{}"); err != nil {
		t.Fatal(err)
	}
	asset := state.tokens[0].ContractAddress
	p := &fakeAptosDepositProvider{head: 2000, rows: map[string][]service.AptosIndexedDeposit{
		owners[1]: {indexedDeposit(owners[1], asset, 1050, 1, "3000000")},
	}, transactions: map[int64][]byte{1050: aptosDepositTransaction(t, owners[1], asset, 1050)}}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err == nil {
		t.Fatal("corrupt cursor was not reported")
	}
	assertAptosRegressionOrderSettled(t, order)
	assertDepositCursor(t, owners[1], service.AptosEventPosition{Version: 2000, Index: math.MaxInt64})
	if stored, _ := data.LoadPaymentCursor("aptos:deposits:" + owners[0]); stored != "{}" {
		t.Fatal("corrupt cursor was silently reset")
	}
}
