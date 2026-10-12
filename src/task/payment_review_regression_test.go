package task

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/GMWalletApp/epusdt/model/service"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestReviewTronPaidOutageMustKeepHealthyPublicScanner(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprintf("paid-nodes-%d", count), func(t *testing.T) {
			testReviewTronPaidOutage(t, count)
		})
	}
}

func testReviewTronPaidOutage(t *testing.T, paidCount int) {
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
	endpoints := []string{public.URL}
	for i := 0; i < paidCount; i++ {
		paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paidCalls.Add(1)
			http.Error(w, "paid temporarily unavailable", http.StatusServiceUnavailable)
		}))
		t.Cleanup(paid.Close)
		endpoints = append(endpoints, paid.URL)
	}
	for i, endpoint := range endpoints {
		purpose := mdb.RpcNodePurposeGeneral
		if i > 0 {
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
	if got.Status != mdb.StatusPaySuccess || publicCalls.Load() != 1 || paidCalls.Load() != int32(min(paidCount, 2)) || scanner.lastBlock != 2 {
		t.Fatalf("near-expiry review failed: order=%+v public=%d paid=%d cursor=%d", got, publicCalls.Load(), paidCalls.Load(), scanner.lastBlock)
	}
}

func TestReviewSchedulerBoundsRoundsAndRotatesSlowAddresses(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	job := &PaymentReviewJob{Network: mdb.NetworkSolana}
	var orders []mdb.Orders
	for i := 0; i < 6; i++ {
		orders = append(orders, mdb.Orders{ReceiveAddress: fmt.Sprintf("wallet-%d", i)})
	}
	orders = append(orders, orders[0]) // same wallet must only consume one slot
	var mu sync.Mutex
	seen := make(map[string]int)
	var active, maxActive int
	for round := 0; round < 3; round++ {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		started := time.Now()
		job.runAddresses(ctx, orders, func(ctx context.Context, address string) error {
			mu.Lock()
			seen[address]++
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			<-ctx.Done()
			mu.Lock()
			active--
			mu.Unlock()
			return ctx.Err()
		})
		cancel()
		if time.Since(started) > time.Second {
			t.Fatal("slow address blocked the whole job")
		}
	}
	if maxActive != 2 || active != 0 || len(seen) != 6 {
		t.Fatalf("starvation/concurrency: max=%d active=%d seen=%v", maxActive, active, seen)
	}
	for address, count := range seen {
		if count != 1 {
			t.Fatalf("wallet %s monopolized retries: %d", address, count)
		}
	}
}

func TestReviewAptosRetryableSettlementDoesNotAdvancePaidCursor(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xb")[0]
	token, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Minute)
	o := &mdb.Orders{TradeId: "paid-cursor-retry", OrderId: "paid-cursor-retry", Network: mdb.NetworkAptos, Token: "USDT", Amount: 21, ActualAmount: 3, Status: mdb.StatusWaitPay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(o, *token, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(o).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	ctx := service.WithPaidPaymentReview(context.Background())
	initial := service.AptosEventPosition{Version: 100, Index: math.MaxInt64}
	if err := saveAptosDepositCursorWithContext(ctx, owner, initial); err != nil {
		t.Fatal(err)
	}
	p := &fakeAptosDepositProvider{rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, token.ContractAddress, 105, 1, "3000000")}}, transactions: map[int64][]byte{105: aptosDepositTransaction(t, owner, token.ContractAddress, 105)}}
	original := processAptosObservedTransferFunc
	t.Cleanup(func() { processAptosObservedTransferFunc = original })
	processAptosObservedTransferFunc = func(service.MoveObservedTransfer) error { return fmt.Errorf("transfer is still being processed") }
	if err := scanAptosOwnerDeposits(ctx, p, owner, []mdb.ChainToken{*token}, 110); err == nil {
		t.Fatal("inflight settlement acknowledged")
	}
	cursor, err := aptosDepositCursorWithContext(ctx, owner)
	if err != nil || cursor != initial {
		t.Fatalf("cursor passed unsettled payment: %+v %v", cursor, err)
	}
	processAptosObservedTransferFunc = original
	if err := scanAptosOwnerDeposits(ctx, p, owner, []mdb.ChainToken{*token}, 110); err != nil {
		t.Fatal(err)
	}
	got, err := data.GetOrderInfoByTradeId(o.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess {
		t.Fatalf("retry did not settle: %+v %v", got, err)
	}
	cursor, err = aptosDepositCursorWithContext(ctx, owner)
	if err != nil || cursor.Version != 110 {
		t.Fatalf("successful retry did not commit cursor: %+v %v", cursor, err)
	}
}
