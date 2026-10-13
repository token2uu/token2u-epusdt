package data

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestAddressBootstrapUsesRecoverableOwnershipNotUpdateTime(t *testing.T) {
	for _, tc := range []struct {
		name, reason, provider string
		status                 int
		wantHistorical         bool
	}{
		{"pending", "", mdb.PaymentProviderOnChain, mdb.StatusWaitPay, true},
		{"naturally_expired", mdb.OrderClosedExpired, mdb.PaymentProviderOnChain, mdb.StatusExpired, true},
		{"manual_close", mdb.OrderClosedManual, "", mdb.StatusExpired, false},
		{"superseded", mdb.OrderClosedSuperseded, "", mdb.StatusExpired, false},
		{"unknown_close", "", "", mdb.StatusExpired, false},
		{"paid", "", "", mdb.StatusPaySuccess, false},
		{"hosted_provider", mdb.OrderClosedExpired, mdb.PaymentProviderOkPay, mdb.StatusExpired, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			now := time.Now().UTC().Truncate(time.Millisecond)
			assigned := now.Add(-24 * time.Hour)
			deadline := assigned.Add(10 * time.Minute)
			order := &mdb.Orders{TradeId: "old-owner", OrderId: "old-owner", Network: mdb.NetworkSolana, ReceiveAddress: "owner", Status: tc.status, ClosedReason: tc.reason, PayProvider: tc.provider, AssignedAtMs: assigned.UnixMilli(), PaymentExpiresAt: &deadline}
			if err := dao.Mdb.Create(order).Error; err != nil {
				t.Fatal(err)
			}
			for _, updated := range []time.Time{deadline, now} {
				if err := dao.Mdb.Model(order).UpdateColumn("updated_at", updated).Error; err != nil {
					t.Fatal(err)
				}
				start, err := PaymentScanStartTime(mdb.NetworkSolana, "owner", now)
				if err != nil || (start.Equal(assigned) != tc.wantHistorical) {
					t.Fatalf("boundary=%v want historical=%t err=%v", start, tc.wantHistorical, err)
				}
			}
			for _, scope := range []struct{ network, address string }{{mdb.NetworkAptos, "owner"}, {mdb.NetworkSolana, "different-owner"}} {
				start, err := PaymentScanStartTime(scope.network, scope.address, now)
				if err != nil || !start.After(assigned) || !start.Before(now) {
					t.Fatalf("unrelated owner inherited boundary=%v err=%v", start, err)
				}
			}
		})
	}
}
