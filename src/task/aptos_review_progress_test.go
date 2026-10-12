package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"gorm.io/gorm"
)

type slowReviewAptosProvider struct {
	*fakeAptosDepositProvider
	ctx context.Context
}

func TestAptosPaidReviewResumesHeadAndPageAfterFailures(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xd")[0]
	asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	order := seedAptosRegressionOrder(t, owner, *asset)
	body := aptosDepositTransaction(t, owner, asset.ContractAddress, 175)
	var stage, headCalls, fetchedPages atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/transactions/by_version/") {
			if stage.Load() < 2 {
				http.Error(w, "RPC unavailable", 503)
				return
			}
			_, _ = w.Write(body)
			return
		}
		var req struct {
			Query     string
			Variables map[string]interface{}
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		response := map[string]interface{}{"ledger_infos": []interface{}{map[string]int{"chain_id": 1}}, "processor_status": []interface{}{map[string]int64{"last_success_version": 180}}}
		if strings.Contains(req.Query, "PaymentDeposits") {
			if stage.Load() == 0 {
				http.Error(w, "indexer page unavailable", 503)
				return
			}
			response["fungible_asset_activities"] = []interface{}{}
			if req.Variables["version"] == "-1" {
				fetchedPages.Add(1)
				response["fungible_asset_activities"] = []interface{}{indexedDeposit(owner, asset.ContractAddress, 175, 1, "3000000")}
			}
		} else {
			headCalls.Add(1)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": response})
	}))
	defer server.Close()
	for _, kind := range []string{mdb.RpcNodeTypeHttp, mdb.RpcNodeTypeIndexer} {
		endpoint := server.URL
		if kind == mdb.RpcNodeTypeIndexer {
			endpoint += "/graphql"
		}
		if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkAptos, Type: kind, Url: endpoint, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
			t.Fatal(err)
		}
	}
	ctx := service.WithPaidPaymentReview(context.Background())
	for round := int32(0); round < 3; round++ {
		stage.Store(round)
		// Simulate retry after endpoint cooldown. Each invocation constructs a
		// fresh provider and can recover only from the database checkpoints.
		data.ResetRpcEndpointPolicyForTest()
		err := reviewAptosPayment(ctx, owner)
		if round < 2 {
			if err == nil {
				t.Fatal("provider failure was not surfaced")
			}
			got, _ := data.GetOrderInfoByTradeId(order.TradeId)
			if got.Status != mdb.StatusWaitPay {
				t.Fatal("unverified payment settled")
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	assertAptosRegressionOrderSettled(t, order)
	if headCalls.Load() != 1 || fetchedPages.Load() != 1 {
		t.Fatalf("completed stages were repeated: head=%d page=%d", headCalls.Load(), fetchedPages.Load())
	}
	if work, err := loadAptosReviewWork(ctx, owner); err != nil || work != nil {
		t.Fatalf("finished work not cleared: %+v %v", work, err)
	}
	if cursor, err := aptosDepositCursorWithContext(ctx, owner); err != nil || cursor != (service.AptosEventPosition{Version: 180, Index: math.MaxInt64}) {
		t.Fatalf("review did not finish frozen interval: %+v %v", cursor, err)
	}
}

func TestAptosPaidReviewCursorWriteFailureReplaysWithoutNewCallback(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xe")[0]
	asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	order := seedAptosRegressionOrder(t, owner, *asset)
	ctx := service.WithPaidPaymentReview(context.Background())
	fake := &fakeAptosDepositProvider{rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset.ContractAddress, 185, 1, "3000000")}}, transactions: map[int64][]byte{185: aptosDepositTransaction(t, owner, asset.ContractAddress, 185)}}
	failCursor := true
	hook := "review_cursor_failure"
	if err := dao.Mdb.Callback().Create().Before("gorm:create").Register(hook, func(tx *gorm.DB) {
		row, ok := tx.Statement.Dest.(*mdb.PaymentScanCursor)
		if ok && failCursor && row.Key == aptosDepositKey(ctx, owner) && strings.Contains(row.Position, "185") {
			tx.AddError(errors.New("injected cursor write failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer dao.Mdb.Callback().Create().Remove(hook)
	if err := scanAptosOwnerDeposits(ctx, fake, owner, []mdb.ChainToken{*asset}, 190); err == nil {
		t.Fatal("cursor failure was not surfaced")
	}
	assertAptosRegressionOrderSettled(t, order)
	if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
		t.Fatal(err)
	}
	failCursor = false
	if err := scanAptosOwnerDeposits(ctx, fake, owner, []mdb.ChainToken{*asset}, 195); err != nil {
		t.Fatal(err)
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk || got.PaidCryptoAmount != "3" {
		t.Fatalf("replay changed settlement or requeued callback: %+v %v", got, err)
	}
	var receipts int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != 1 {
		t.Fatalf("replay duplicated receipt: %d %v", receipts, err)
	}
	cursor, err := aptosDepositCursorWithContext(ctx, owner)
	if err != nil || cursor.Version != 190 {
		t.Fatalf("resume changed frozen interval: %+v %v", cursor, err)
	}
}

func (p *slowReviewAptosProvider) Deposits(ctx context.Context, owner string, after service.AptosEventPosition, head int64, limit int, since string) ([]service.AptosIndexedDeposit, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(4200 * time.Millisecond):
	}
	return p.fakeAptosDepositProvider.Deposits(ctx, owner, after, head, limit, since)
}
func (p *slowReviewAptosProvider) Transaction(v int64, validate func([]byte) error) ([]byte, error) {
	select {
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case <-time.After(4200 * time.Millisecond):
	}
	return p.fakeAptosDepositProvider.Transaction(v, validate)
}
func TestAptosPaidReviewRetainsPartialPageProgress(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xc")[0]
	token, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Minute)
	o := &mdb.Orders{TradeId: "review2-aptos", OrderId: "review2-aptos", Network: mdb.NetworkAptos, Token: "USDT", Amount: 21, ActualAmount: 3, Status: mdb.StatusWaitPay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(o, *token, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(o).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	oldBody := aptosDepositTransaction(t, owner, token.ContractAddress, 105)
	var old map[string]interface{}
	if err := json.Unmarshal(oldBody, &old); err != nil {
		t.Fatal(err)
	}
	old["timestamp"] = fmt.Sprint(time.Now().Add(-2 * time.Minute).UnixMicro())
	oldBody, _ = json.Marshal(old)
	fake := &fakeAptosDepositProvider{rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, token.ContractAddress, 105, 1, "3000000"), indexedDeposit(owner, token.ContractAddress, 106, 1, "3000000")}}, transactions: map[int64][]byte{105: oldBody, 106: aptosDepositTransaction(t, owner, token.ContractAddress, 106)}}
	for round := 0; round < 2; round++ {
		ctx, cancel := context.WithTimeout(service.WithPaidPaymentReview(context.Background()), 12*time.Second)
		p := &slowReviewAptosProvider{fakeAptosDepositProvider: fake, ctx: ctx}
		err := scanAptosOwnerDeposits(ctx, p, owner, []mdb.ChainToken{*token}, 110)
		cancel()
		t.Logf("round=%d err=%v", round, err)
	}
	got, _ := data.GetOrderInfoByTradeId(o.TradeId)
	cursor, _ := aptosDepositCursorWithContext(service.WithPaidPaymentReview(context.Background()), owner)
	if got.Status != mdb.StatusPaySuccess {
		t.Fatalf("paid review repeats partial page forever: status=%d cursor=%+v verifiedTransactions=%d", got.Status, cursor, fake.transactionCalls)
	}
}
