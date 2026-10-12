package mq

import (
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestExclusiveCallbackRetriesActualAmountAndClosesLatePayment(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	callbackLimiter = make(chan struct{}, 1)
	callbackInflight = sync.Map{}
	defer waitForCallbackWorkers(t)
	requests := make(chan url.Values, 8)
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Query()
		count++
		if count == 1 {
			w.WriteHeader(503)
			return
		}
		_, _ = io.WriteString(w, "success")
	}))
	defer server.Close()
	expires := time.Now().Add(time.Minute)
	order := &mdb.Orders{TradeId: "exclusive-callback", OrderId: "merchant-order", Network: "tron", Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires, NotifyUrl: server.URL, ApiKeyID: 2}
	if err := data.CreateExclusivePayment(order, mdb.ChainToken{ContractAddress: "usdt", Decimals: 6}, []mdb.WalletAddress{{Address: "wallet"}}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := data.ProcessExclusivePayment(data.ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", AssetID: "usdt", TxHash: "first", EventID: "0", RawAmount: big.NewInt(3_000_000), Decimals: 6, BlockTimeMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	dispatchPendingCallbacks()
	waitFor(t, 3*time.Second, func() bool { current, _ := data.GetOrderInfoByTradeId(order.TradeId); return current.CallbackNum == 1 })
	// Simulate a subsequent worker run after the persisted backoff has elapsed.
	dao.Mdb.Model(order).UpdateColumn("updated_at", time.Now().Add(-2*time.Second))
	dispatchPendingCallbacks()
	waitFor(t, 3*time.Second, func() bool {
		current, _ := data.GetOrderInfoByTradeId(order.TradeId)
		return current.CallbackNum == 2 && current.CallBackConfirm == mdb.CallBackConfirmOk
	})
	for i := 0; i < 2; i++ {
		select {
		case q := <-requests:
			if q.Get("money") != "35.0000" || q.Get("paid_money") != "21.0000" || q.Get("trade_status") != "TRADE_SUCCESS" {
				t.Fatalf("incorrect callback: %v", q)
			}
		case <-time.After(time.Second):
			t.Fatal("missing callback")
		}
	}
	// A second payment produces an exception and cannot requeue a paid order.
	if _, err := data.ProcessExclusivePayment(data.ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", AssetID: "usdt", TxHash: "second", EventID: "0", RawAmount: big.NewInt(6_000_000), Decimals: 6, BlockTimeMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	current, _ := data.GetOrderInfoByTradeId(order.TradeId)
	if current.PaidAmount != 21 || current.CallbackNum != 2 || current.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatal("second payment requeued callback")
	}
	// Expiration keeps TRADE_CLOSED; a late receipt never revives the order.
	late := &mdb.Orders{TradeId: "late-callback", OrderId: "late-merchant", Network: "tron", Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires, NotifyUrl: server.URL, ApiKeyID: 2}
	if err := data.CreateExclusivePayment(late, mdb.ChainToken{ContractAddress: "usdt", Decimals: 6}, []mdb.WalletAddress{{Address: "other-wallet"}}, false); err != nil {
		t.Fatal(err)
	}
	dao.Mdb.Model(late).Update("payment_expires_at", time.Now().Add(-time.Second))
	processExpiredOrders()
	dispatchPendingCallbacks()
	waitFor(t, 3*time.Second, func() bool {
		current, _ := data.GetOrderInfoByTradeId(late.TradeId)
		return current.CallBackConfirm == mdb.CallBackConfirmOk
	})
	select {
	case q := <-requests:
		if q.Get("trade_status") != "TRADE_CLOSED" || q.Get("paid_money") != "" {
			t.Fatalf("incorrect close callback: %v", q)
		}
	case <-time.After(time.Second):
		t.Fatal("missing close callback")
	}
	if _, err := data.ProcessExclusivePayment(data.ObservedPayment{Network: "tron", Address: "other-wallet", Token: "USDT", AssetID: "usdt", TxHash: "late", EventID: "0", RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	current, _ = data.GetOrderInfoByTradeId(late.TradeId)
	if current.Status != mdb.StatusExpired || current.CallBackConfirm != mdb.CallBackConfirmOk || current.PaidAmount != 0 {
		t.Fatal("late payment revived closed order")
	}
}

func TestRecoveredPaymentCorrectsInflightCloseAndRetriesIdempotently(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	callbackLimiter = make(chan struct{}, 1)
	callbackInflight = sync.Map{}
	defer waitForCallbackWorkers(t)
	closedStarted, releaseClosed := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	release := func() { unblock.Do(func() { close(releaseClosed) }) }
	defer release()
	requests := make(chan url.Values, 8)
	paidAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		requests <- q
		if q.Get("trade_status") == "TRADE_CLOSED" {
			close(closedStarted)
			select {
			case <-releaseClosed:
			case <-time.After(3 * time.Second):
			}
		} else {
			paidAttempts++
			if paidAttempts == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = io.WriteString(w, "success")
	}))
	defer server.Close()
	expires := time.Now().Add(time.Minute)
	order := &mdb.Orders{TradeId: "recovered-callback", OrderId: "merchant-original", Network: "tron", Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires, NotifyUrl: server.URL, ApiKeyID: 2}
	if err := data.CreateExclusivePayment(order, mdb.ChainToken{ContractAddress: "usdt", Decimals: 6}, []mdb.WalletAddress{{Address: "wallet"}}, false); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"assigned_at_ms": now.Add(-3 * time.Minute).UnixMilli(), "payment_expires_at": now.Add(-time.Minute)}).Error; err != nil {
		t.Fatal(err)
	}
	processExpiredOrders()
	dispatchPendingCallbacks()
	select {
	case <-closedStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("close callback did not start")
	}
	p := data.ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", AssetID: "usdt", TxHash: "paid-before-expiry", EventID: "0", RawAmount: big.NewInt(3_000_000), Decimals: 6, BlockTimeMs: now.Add(-2 * time.Minute).UnixMilli()}
	if handled, err := data.ProcessExclusivePayment(p); !handled || err != nil {
		t.Fatalf("recovery handled=%v err=%v", handled, err)
	}
	release()
	waitForCallbackWorkers(t)
	current, _ := data.GetOrderInfoByTradeId(order.TradeId)
	if current.Status != mdb.StatusPaySuccess || current.CallBackConfirm != mdb.CallBackConfirmNo || current.CallbackNum != 0 {
		t.Fatalf("stale close acknowledgement consumed success callback: %+v", current)
	}
	dispatchPendingCallbacks()
	waitFor(t, 3*time.Second, func() bool { got, _ := data.GetOrderInfoByTradeId(order.TradeId); return got.CallbackNum == 1 })
	waitForCallbackWorkers(t)
	if err := dao.Mdb.Model(order).UpdateColumn("updated_at", time.Now().Add(-2*time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	dispatchPendingCallbacks()
	waitForCallbackWorkers(t)
	current, _ = data.GetOrderInfoByTradeId(order.TradeId)
	if current.CallBackConfirm != mdb.CallBackConfirmOk || current.CallbackNum != 2 || current.PaidAmount != 21 {
		t.Fatalf("success retry failed: %+v", current)
	}
	for i := 0; i < 3; i++ {
		q := <-requests
		if q.Get("trade_no") != order.TradeId || q.Get("out_trade_no") != order.OrderId {
			t.Fatalf("callback changed original identity: %v", q)
		}
		if i > 0 && (q.Get("trade_status") != "TRADE_SUCCESS" || q.Get("paid_money") != "21.0000") {
			t.Fatalf("incorrect corrected callback: %v", q)
		}
	}
	if _, err := data.ProcessExclusivePayment(p); err != nil {
		t.Fatal(err)
	}
	dispatchPendingCallbacks()
	waitForCallbackWorkers(t)
	current, _ = data.GetOrderInfoByTradeId(order.TradeId)
	if current.CallbackNum != 2 || current.CallBackConfirm != mdb.CallBackConfirmOk || len(requests) != 0 {
		t.Fatal("receipt replay requeued acknowledged callback")
	}
}

func TestExpirationDoesNotReleaseReassignedLegacyAmount(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	old := &mdb.Orders{TradeId: "old-reservation", OrderId: "old-reservation", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Status: mdb.StatusWaitPay, ActualAmount: 5}
	if err := dao.Mdb.Create(old).Error; err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(old).UpdateColumn("created_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	// The runtime lock has already expired independently of the order queue.
	// A new order now owns the same network/address/token/amount reservation.
	if err := data.LockTransaction("tron", "wallet", "USDT", "new-reservation", 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	processExpiredOrders()
	current, _ := data.GetOrderInfoByTradeId(old.TradeId)
	if current.Status != mdb.StatusExpired || current.ReleasedAtMs == 0 {
		t.Fatalf("original order not expired: %+v", current)
	}
	if id, err := data.GetTradeIdByWalletAddressAndAmountAndToken("tron", "wallet", "USDT", 5); err != nil || id != "new-reservation" {
		t.Fatalf("expired order released new owner's lock: %s %v", id, err)
	}
}
