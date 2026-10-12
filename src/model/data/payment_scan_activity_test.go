package data

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestPaymentScanActivityTracksOrdersAndLateReceipts(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	now := time.Now()
	old := now.Add(-time.Hour)
	future := now.Add(time.Minute)
	expired := now.Add(-time.Minute)
	for _, tc := range []struct {
		name, network string
		status        int
		expiry        *time.Time
		created       time.Time
		released      int64
		want          bool
	}{
		{"pending", mdb.NetworkSolana, mdb.StatusWaitPay, &future, old, 0, true},
		{"legacy", mdb.NetworkSolana, mdb.StatusWaitPay, nil, now.Add(-time.Minute), 0, true},
		{"old-expired", mdb.NetworkSolana, mdb.StatusWaitPay, &expired, old, 0, false},
		{"released", mdb.NetworkSolana, mdb.StatusExpired, &expired, old, now.UnixMilli(), true},
		{"other-chain", mdb.NetworkAptos, mdb.StatusWaitPay, &future, old, 0, false},
	} {
		order := &mdb.Orders{TradeId: tc.name, OrderId: tc.name, ReceiveAddress: tc.name, Network: tc.network, Status: tc.status, PaymentExpiresAt: tc.expiry, ReleasedAtMs: tc.released}
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
		if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"created_at": tc.created, "updated_at": old}).Error; err != nil {
			t.Fatal(err)
		}
		got, err := RecentlyActivePaymentAddresses(mdb.NetworkSolana, now, 10*time.Minute)
		if err != nil || got[tc.name] != tc.want {
			t.Fatalf("%s: got active=%v, err=%v", tc.name, got[tc.name], err)
		}
	}
	if err := dao.Mdb.Create(&mdb.PaymentReceipt{EventKey: "late-receipt", Network: mdb.NetworkSolana, Address: "CaseSensitiveOwner", CreatedAt: now}).Error; err != nil {
		t.Fatal(err)
	}
	got, err := RecentlyActivePaymentAddresses(mdb.NetworkSolana, now, 10*time.Minute)
	if err != nil || !got["CaseSensitiveOwner"] || got["casesensitiveowner"] {
		t.Fatalf("receipt activity/case lost: %v, %v", got, err)
	}
}

func TestPaymentScanStartCoversOldPendingOrder(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-2 * time.Hour)
	order := &mdb.Orders{TradeId: "old-pending", OrderId: "old-pending", Network: mdb.NetworkAptos, ReceiveAddress: "0xa", Status: mdb.StatusWaitPay}
	if err := dao.Mdb.Create(order).Error; err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(order).Update("created_at", old).Error; err != nil {
		t.Fatal(err)
	}
	start, err := PaymentScanStartTime(mdb.NetworkAptos, "0xa", now)
	if err != nil || !start.Equal(old) {
		t.Fatalf("start=%v want=%v err=%v", start, old, err)
	}
	other, err := PaymentScanStartTime(mdb.NetworkAptos, "0xb", now)
	if err != nil || !other.After(old) || !other.Before(now) {
		t.Fatalf("unrelated address inherited old order: %v err=%v", other, err)
	}
}
