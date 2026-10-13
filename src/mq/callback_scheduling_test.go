package mq

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func TestCallbackBackoffDoesNotBlockReadyPayments(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	viper.Set("order_notice_max_retry", 20)
	callbackLimiter = make(chan struct{}, 4)
	callbackInflight = sync.Map{}
	requests := make(chan string, batchSize+10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Query().Get("out_trade_no")
		_, _ = w.Write([]byte("success"))
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { waitForCallbackWorkers(t) })
	create := func(id string, attempt int, age time.Duration) {
		t.Helper()
		order := &mdb.Orders{TradeId: id, OrderId: id, Amount: 35, ActualAmount: 5, Token: "USDT", Network: mdb.NetworkTron,
			Status: mdb.StatusPaySuccess, PaymentType: mdb.PaymentTypeEpay, NotifyUrl: server.URL, ApiKeyID: 2,
			CallbackNum: attempt, CallBackConfirm: mdb.CallBackConfirmNo}
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
		if err := dao.Mdb.Model(order).UpdateColumn("updated_at", time.Now().Add(-age)).Error; err != nil {
			t.Fatal(err)
		}
	}
	// These older rows are waiting for exponential/capped retry delays. They
	// must not occupy the entire SQL batch ahead of callbacks that are due.
	for i := 0; i < batchSize; i++ {
		attempt := 7
		if i%2 == 0 {
			attempt = 15
		}
		create(fmt.Sprintf("backoff-%d", i), attempt, 10*time.Second)
	}
	create("ready-first", 0, 0)
	create("ready-retry", 2, 5*time.Second)
	dispatchPendingCallbacks()
	waitForCallbackWorkers(t)
	for _, id := range []string{"ready-first", "ready-retry"} {
		order, err := data.GetOrderInfoByTradeId(id)
		if err != nil || order.CallBackConfirm != mdb.CallBackConfirmOk {
			t.Fatalf("due payment callback blocked behind backoff rows: %s %+v %v", id, order, err)
		}
	}
	if len(requests) != 2 {
		t.Fatalf("unexpected requests: %d", len(requests))
	}
	for len(requests) > 0 {
		id := <-requests
		if id != "ready-first" && id != "ready-retry" {
			t.Fatalf("callback sent before retry deadline: %s", id)
		}
	}
	dispatchPendingCallbacks()
	waitForCallbackWorkers(t)
	if len(requests) != 0 {
		t.Fatal("acknowledged callback repeated or backoff bypassed")
	}
}
