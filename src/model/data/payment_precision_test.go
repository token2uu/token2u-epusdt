package data

import (
	"fmt"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"math/big"
	"testing"
	"time"
)

func TestReviewRecoveryHonorsExistingLockPrecision(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "4", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	for i, amount := range []float64{5.0001, 5.0002} {
		id := []string{"first", "second"}[i]
		reviewLegacy(t, id, mdb.StatusWaitPay, amount, now.Add(-time.Minute), now.Add(time.Minute), "")
		if err := LockTransaction("tron", "wallet", "USDT", id, amount, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	match, err := GetTradeIdByWalletAddressAndAmountAndTokenWithEpayFallback("tron", "wallet", "USDT", 5.0001)
	if err != nil || match.TradeId != "first" {
		t.Fatalf("existing lock match invalid: %+v %v", match, err)
	}
	p := reviewPayment(now)
	p.RawAmount = big.NewInt(5_000_100)
	handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
	if err != nil {
		t.Fatal(err)
	}
	var receipt mdb.PaymentReceipt
	dao.Mdb.First(&receipt)
	if !handled || settled == nil || settled.TradeId != "first" {
		t.Fatalf("uniquely matching payment blocked after precision change: handled=%v settled=%+v receipt=%s", handled, settled, receipt.Reason)
	}
}

func TestReviewNewReservationPrecisionSurvivesExpiryAndSettingChange(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "4", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	if err := LockTransaction("tron", "wallet", "USDT", "snapshot", 5.0001, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	o := reviewLegacy(t, "snapshot", mdb.StatusWaitPay, 5.0001, now.Add(-2*time.Minute), now.Add(-time.Minute), "")
	// Recreate through the real order creation path after the setting changed.
	if err := dao.Mdb.Unscoped().Delete(o).Error; err != nil {
		t.Fatal(err)
	}
	o.ID = 0
	if err := CreateOrderWithTransaction(dao.Mdb, o); err != nil {
		t.Fatal(err)
	}
	if o.AmountPrecision == nil || *o.AmountPrecision != 4 {
		t.Fatalf("reservation precision was not snapshotted: %+v", o.AmountPrecision)
	}
	if err := dao.Mdb.Model(o).Updates(map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedExpired}).Error; err != nil {
		t.Fatal(err)
	}
	if err := UnLockTransactionByTradeId(o.TradeId); err != nil {
		t.Fatal(err)
	}
	// The old four-decimal rounding tolerance still applies after lock deletion.
	p := reviewPayment(now.Add(-90 * time.Second))
	p.RawAmount = big.NewInt(5_000_110)
	handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
	if err != nil || !handled || settled == nil || settled.TradeId != o.TradeId {
		t.Fatalf("late payment lost original precision: %v %+v %v", handled, settled, err)
	}
	_, replay, err := SettleLegacyObservedPaymentWithResult(p)
	if err != nil || replay != nil {
		t.Fatalf("replay settled twice: %+v %v", replay, err)
	}
}

func TestReviewPlaceholderSnapshotsPrecisionWhenAssigned(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	o := &mdb.Orders{TradeId: "placeholder", OrderId: "placeholder", Status: mdb.StatusWaitSelect, Amount: 35}
	if err := CreateOrderWithTransaction(dao.Mdb, o); err != nil {
		t.Fatal(err)
	}
	if o.AmountPrecision != nil {
		t.Fatal("unassigned order has no reservation precision")
	}
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "4", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	if err := LockTransaction("tron", "wallet", "USDT", o.TradeId, 5.0001, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeInt); err != nil {
		t.Fatal(err)
	}
	updated, err := CompleteWaitSelectOrder(o.TradeId, "tron", "USDT", "wallet", 5.0001)
	got, loadErr := GetOrderInfoByTradeId(o.TradeId)
	if err != nil || !updated || loadErr != nil || got.AmountPrecision == nil || *got.AmountPrecision != 4 {
		t.Fatalf("assignment precision: updated=%v order=%+v errors=%v/%v", updated, got, err, loadErr)
	}
}

func TestReviewUnknownHistoricalPrecisionDoesNotMergeOrRoundAmounts(t *testing.T) {
	for _, raw := range []int64{5_000_100, 5_000_000} {
		t.Run(fmt.Sprint(raw), func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			now := time.Now()
			first := reviewLegacy(t, "unknown-first", mdb.StatusExpired, 5.0001, now.Add(-2*time.Minute), now.Add(-time.Minute), "")
			reviewLegacy(t, "unknown-second", mdb.StatusExpired, 5.0002, now.Add(-2*time.Minute), now.Add(-time.Minute), "")
			if err := dao.Mdb.Model(first).Update("closed_reason", mdb.OrderClosedExpired).Error; err != nil {
				t.Fatal(err)
			}
			p := reviewPayment(now.Add(-90 * time.Second))
			p.RawAmount = big.NewInt(raw)
			handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
			if err != nil {
				t.Fatal(err)
			}
			if raw == 5_000_100 && (!handled || settled == nil || settled.TradeId != first.TradeId) {
				t.Fatalf("exact payment not recovered: %v %+v", handled, settled)
			}
			if raw == 5_000_000 {
				var receipt mdb.PaymentReceipt
				if err := dao.Mdb.First(&receipt).Error; err != nil {
					t.Fatal(err)
				}
				if !handled || settled != nil || receipt.Status != "exception" || receipt.TradeID != "" {
					t.Fatalf("unknown precision must retain unassigned funds: %v %+v %+v", handled, settled, receipt)
				}
			}
		})
	}
}
