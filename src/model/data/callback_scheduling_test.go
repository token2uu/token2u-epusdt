package data

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func TestDueCallbackBatchPreservesRetryPolicy(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	viper.Set("callback_retry_base_seconds", 5)
	now := time.Now()
	want := map[string]bool{}
	for _, tc := range []struct {
		id      string
		attempt int
		age     time.Duration
		due     bool
	}{
		{"first", 0, 0, true},
		{"retry-one-ready", 1, 6 * time.Second, true},
		{"retry-one-waiting", 1, 3 * time.Second, false},
		{"retry-two-ready", 2, 11 * time.Second, true},
		{"retry-two-waiting", 2, 8 * time.Second, false},
		{"retry-six-ready", 6, 161 * time.Second, true},
		{"retry-six-waiting", 6, 158 * time.Second, false},
		{"capped-ready", 7, 301 * time.Second, true},
		{"capped-waiting", 7, 298 * time.Second, false},
		{"last-retry-ready", 20, 310 * time.Second, true},
		{"last-retry-waiting", 20, 290 * time.Second, false},
		{"exhausted", 21, time.Hour, false},
	} {
		order := &mdb.Orders{TradeId: tc.id, OrderId: tc.id, Status: mdb.StatusPaySuccess,
			CallbackNum: tc.attempt, CallBackConfirm: mdb.CallBackConfirmNo, NotifyUrl: "https://merchant.example/callback"}
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
		if err := dao.Mdb.Model(order).UpdateColumn("updated_at", now.Add(-tc.age)).Error; err != nil {
			t.Fatal(err)
		}
		if tc.due {
			want[tc.id] = true
		}
	}
	// Existing filters must still apply to every OR branch in the due predicate.
	for _, id := range []string{"acknowledged", "pending", "no-notify"} {
		order := &mdb.Orders{TradeId: id, OrderId: id, Status: mdb.StatusExpired, CallbackNum: 20,
			CallBackConfirm: mdb.CallBackConfirmNo, NotifyUrl: "https://merchant.example/callback"}
		switch id {
		case "acknowledged":
			order.CallBackConfirm = mdb.CallBackConfirmOk
		case "pending":
			order.Status = mdb.StatusWaitPay
		case "no-notify":
			order.NotifyUrl = ""
		}
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
		if err := dao.Mdb.Model(order).UpdateColumn("updated_at", now.Add(-time.Hour)).Error; err != nil {
			t.Fatal(err)
		}
	}
	rows, err := GetPendingCallbackOrders(20, 100)
	if err != nil || len(rows) != len(want) {
		t.Fatalf("due callbacks=%+v want=%v err=%v", rows, want, err)
	}
	for _, row := range rows {
		if !want[row.TradeId] {
			t.Fatalf("ineligible callback selected: %+v", row)
		}
		delete(want, row.TradeId)
	}
	rows, err = GetPendingCallbackOrders(20, 1)
	if err != nil || len(rows) != 1 || rows[0].TradeId != "last-retry-ready" {
		t.Fatalf("batch ordering/limit changed: %+v %v", rows, err)
	}
	rows, err = GetPendingCallbackOrders(0, 100)
	if err != nil || len(rows) != 1 || rows[0].TradeId != "first" {
		t.Fatalf("zero retries must still send the initial notification: %+v %v", rows, err)
	}
}
