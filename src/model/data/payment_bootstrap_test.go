package data

import (
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"testing"
	"time"
)

func TestNetworkBootstrapCoversExpiredOrdersAndSurvivesLongOutage(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	now := time.Now().UTC().Truncate(time.Millisecond)
	assigned := now.Add(-3 * time.Hour)
	deadline := assigned.Add(time.Minute)
	rows := []mdb.Orders{
		{TradeId: "expired-before-first-scan", OrderId: "expired-before-first-scan", Network: mdb.NetworkTron, ReceiveAddress: "wallet", Status: mdb.StatusExpired, ClosedReason: mdb.OrderClosedExpired, AssignedAtMs: assigned.UnixMilli(), PaymentExpiresAt: &deadline},
		{TradeId: "unrelated", OrderId: "unrelated", Network: mdb.NetworkTon, ReceiveAddress: "wallet", Status: mdb.StatusWaitPay, AssignedAtMs: assigned.Add(-time.Hour).UnixMilli()},
		{TradeId: "manual", OrderId: "manual", Network: mdb.NetworkTron, ReceiveAddress: "wallet", Status: mdb.StatusExpired, ClosedReason: mdb.OrderClosedManual, AssignedAtMs: assigned.Add(-time.Hour).UnixMilli()},
	}
	if err := dao.Mdb.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	first, err := EnsureNetworkScanStartTime(mdb.NetworkTron, now)
	if err != nil || !first.Equal(assigned) {
		t.Fatalf("boundary=%v want=%v err=%v", first, assigned, err)
	}
	// Order/MQ changes and a much later restart cannot slide the persisted start.
	if err := dao.Mdb.Model(&rows[0]).Update("status", mdb.StatusPaySuccess).Error; err != nil {
		t.Fatal(err)
	}
	second, err := EnsureNetworkScanStartTime(mdb.NetworkTron, now.Add(24*time.Hour))
	if err != nil || !second.Equal(first) {
		t.Fatalf("outage moved boundary: %v %v", second, err)
	}
	if err := SavePaymentCursor(mdb.NetworkTron+":bootstrap_time", "corrupt"); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureNetworkScanStartTime(mdb.NetworkTron, now); err == nil {
		t.Fatal("corrupt checkpoint silently replaced")
	}
}
