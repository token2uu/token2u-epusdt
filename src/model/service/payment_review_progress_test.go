package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/dromara/carbon/v2"
	"gorm.io/gorm"
)

func TestReviewAptosInflightRetriesAfterFailedSettlement(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	owner, _ := addressutil.NormalizeMoveAddress("0xa")
	o := &mdb.Orders{TradeId: "inflight", OrderId: "inflight", Network: mdb.NetworkAptos, Token: "USDT", ReceiveAddress: owner, Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay}
	o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-time.Minute)))
	if err := data.LockTransaction(o.Network, owner, o.Token, o.TradeId, 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := data.CreateOrderWithTransaction(dao.Mdb, o); err != nil {
		t.Fatal(err)
	}
	transfer := MoveObservedTransfer{Network: o.Network, ReceiveAddress: owner, Token: o.Token, RawAmount: big.NewInt(5_000_000), Decimals: 6, Amount: 5, TxID: "inflight-payment", TransferKey: "deep-review-inflight", BlockTimeMs: now.UnixMilli()}
	gProcessedMoveTx.Delete(transfer.TransferKey)
	defer gProcessedMoveTx.Delete(transfer.TransferKey)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := "fail_inflight_settlement"
	if err := dao.Mdb.Callback().Update().Before("gorm:update").Register(hook, func(tx *gorm.DB) {
		updates, ok := tx.Statement.Dest.(map[string]interface{})
		if !ok || tx.Statement.Table != "orders" || updates["status"] != mdb.StatusPaySuccess {
			return
		}
		once.Do(func() { close(entered) })
		<-release
		tx.AddError(errors.New("transient write failure"))
	}); err != nil {
		t.Fatal(err)
	}
	defer dao.Mdb.Callback().Update().Remove(hook)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- ProcessMoveObservedTransferResult(transfer) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("did not enter pending settlement")
	}
	go func() { second <- ProcessMoveObservedTransferResult(transfer) }()
	var secondErr error
	select {
	case secondErr = <-second:
	case <-time.After(2 * time.Second):
		close(release)
		<-first
		t.Fatal("concurrent scanner must receive retryable result")
	}
	close(release)
	firstErr := <-first
	got, err := data.GetOrderInfoByTradeId(o.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if firstErr == nil {
		t.Fatal("failure fixture did not fail")
	}
	if secondErr == nil || got.Status != mdb.StatusWaitPay {
		t.Fatalf("inflight scan must retry: status=%d first=%v second=%v", got.Status, firstErr, secondErr)
	}
	dao.Mdb.Callback().Update().Remove(hook)
	if err := ProcessMoveObservedTransferResult(transfer); err != nil {
		t.Fatal(err)
	}
	got, err = data.GetOrderInfoByTradeId(o.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess {
		t.Fatalf("retry did not settle: order=%+v err=%v", got, err)
	}
	if err := ProcessMoveObservedTransferResult(transfer); err != nil {
		t.Fatal(err)
	}
	replay, _ := data.GetOrderInfoByTradeId(o.TradeId)
	if replay.BlockTransactionId != transfer.TxID || replay.CallbackNum != got.CallbackNum {
		t.Fatalf("replay changed settlement: %+v", replay)
	}
}

func TestReviewPaidSolanaBudgetEventuallyMakesProgress(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	order := solCursorOrder(t)
	l.add("budget-payment", 2, "3000000")
	// Every response is successful in 4.2s, below the paid RPC's 10s timeout.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(4200 * time.Millisecond):
		}
		l.ServeHTTP(w, r)
	}))
	defer server.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(WithPaidPaymentReview(context.Background()), 12*time.Second)
		err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner)
		cancel()
		t.Logf("round %d: %v", i, err)
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	cursor, _ := data.LoadPaymentCursor("solana:payment_review:" + l.ata)
	if got.Status != mdb.StatusPaySuccess {
		t.Fatalf("healthy paid node cannot finish any round or retain progress: status=%d cursor=%q", got.Status, cursor)
	}
}

func TestReviewSolanaRestartResumesTransactionsAndKeepsNewHead(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	now := time.Now()
	for i, amount := range []float64{5, 6, 7} {
		id := fmt.Sprintf("resume-%d", i)
		o := &mdb.Orders{TradeId: id, OrderId: id, Network: mdb.NetworkSolana, Token: "USDT", ReceiveAddress: solCursorTestOwner, Amount: 35, ActualAmount: amount, Status: mdb.StatusWaitPay}
		o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-time.Minute)))
		if err := data.LockTransaction(o.Network, o.ReceiveAddress, o.Token, o.TradeId, amount, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := data.CreateOrderWithTransaction(dao.Mdb, o); err != nil {
			t.Fatal(err)
		}
	}
	l.add("resume-first", 1, "5000000")
	l.add("resume-second", 2, "6000000")
	var failSecond atomic.Bool
	failSecond.Store(true)
	var firstCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if json.Unmarshal(body, &req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		var sig string
		if req.Method == "getTransaction" && len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params[0], &sig)
			if sig == "resume-first" {
				firstCalls.Add(1)
			}
			if sig == "resume-second" && failSecond.Load() {
				<-r.Context().Done()
				return
			}
		}
		l.ServeHTTP(w, r)
	}))
	defer server.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(WithPaidPaymentReview(context.Background()), time.Second)
	err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner)
	cancel()
	if err == nil {
		t.Fatal("incomplete second transaction accepted")
	}
	first, _ := data.GetOrderInfoByTradeId("resume-0")
	second, _ := data.GetOrderInfoByTradeId("resume-1")
	cursor, _ := data.LoadPaymentCursor("solana:payment_review:" + l.ata)
	if first.Status != mdb.StatusPaySuccess || second.Status != mdb.StatusWaitPay || cursor != "" {
		t.Fatalf("partial round corrupted settlement: first=%d second=%d cursor=%q", first.Status, second.Status, cursor)
	}
	// Model a callback acknowledgement and a restart after the first settlement.
	if err := dao.Mdb.Model(first).Update("callback_confirm", mdb.CallBackConfirmOk).Error; err != nil {
		t.Fatal(err)
	}
	clearProcessedSignatures()
	failSecond.Store(false)
	l.add("resume-new-head", 3, "7000000")
	ctx, cancel = context.WithTimeout(WithPaidPaymentReview(context.Background()), 5*time.Second)
	defer cancel()
	if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	first, _ = data.GetOrderInfoByTradeId("resume-0")
	second, _ = data.GetOrderInfoByTradeId("resume-1")
	third, _ := data.GetOrderInfoByTradeId("resume-2")
	if first.CallBackConfirm != mdb.CallBackConfirmOk || firstCalls.Load() != 1 || second.Status != mdb.StatusPaySuccess || third.Status != mdb.StatusWaitPay {
		t.Fatalf("resume replayed/skipped work: first=%+v calls=%d second=%d third=%d", first, firstCalls.Load(), second.Status, third.Status)
	}
	if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	third, _ = data.GetOrderInfoByTradeId("resume-2")
	cursor, _ = data.LoadPaymentCursor("solana:payment_review:" + l.ata)
	if third.Status != mdb.StatusPaySuccess || cursor != "resume-new-head" {
		t.Fatalf("new head lost during resume: status=%d cursor=%q", third.Status, cursor)
	}
}

func TestReviewSolanaRejectsIncompleteCheckpoint(t *testing.T) {
	l := setupSolCursorLedger(t)
	o := solCursorOrder(t)
	l.add("unsettled-checkpoint", 1, "3000000")
	for _, stored := range []string{"{}", `{"Version":1,"Queries":[],"Processed":{}}`, "not-json"} {
		if err := data.SavePaymentCursor(solReviewWorkKey(solCursorTestOwner), stored); err != nil {
			t.Fatal(err)
		}
		if err := ScanSolanaAddressWithContext(WithPaidPaymentReview(context.Background()), solCursorTestOwner); err == nil {
			t.Fatalf("invalid checkpoint accepted: %s", stored)
		}
		got, _ := data.GetOrderInfoByTradeId(o.TradeId)
		cursor, _ := data.LoadPaymentCursor("solana:payment_review:" + l.ata)
		if got.Status != mdb.StatusWaitPay || cursor != "" {
			t.Fatal("invalid checkpoint advanced settlement/cursor")
		}
	}
}
