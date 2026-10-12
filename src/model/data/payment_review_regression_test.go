package data

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/dromara/carbon/v2"
	"github.com/spf13/viper"
)

func reviewLegacy(t *testing.T, id string, status int, amount float64, created, expiry time.Time, notify string) *mdb.Orders {
	t.Helper()
	o := &mdb.Orders{TradeId: id, OrderId: id, Status: status, Network: "tron", Token: "USDT", ReceiveAddress: "wallet", ActualAmount: amount, Amount: 35, Currency: "CNY", NotifyUrl: notify, PaymentExpiresAt: &expiry}
	o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(created))
	if err := dao.Mdb.Create(o).Error; err != nil {
		t.Fatal(err)
	}
	return o
}
func reviewPayment(at time.Time) ObservedPayment {
	return ObservedPayment{Network: "tron", Token: "USDT", Address: "wallet", AssetID: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", RawAmount: big.NewInt(5_000_000), Decimals: 6, TxHash: "current-payment", EventID: "0", BlockTimeMs: at.UnixMilli()}
}

func TestReviewExactLegacyPaymentMustKeepExactPriority(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	for i, amount := range []float64{5, 5.01} {
		id := []string{"exact", "other"}[i]
		reviewLegacy(t, id, mdb.StatusWaitPay, amount, now.Add(-time.Minute), now.Add(time.Minute), "https://merchant.test/epay/notify")
		if err := LockTransaction("tron", "wallet", "USDT", id, amount, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	match, err := GetTradeIdByWalletAddressAndAmountAndTokenWithEpayFallback("tron", "wallet", "USDT", 5)
	if err != nil || match.TradeId != "exact" {
		t.Fatalf("fixture must have exact match: %+v %v", match, err)
	}
	if handled, settled, err := SettleLegacyObservedPaymentWithResult(reviewPayment(now)); err != nil || !handled || settled == nil || settled.TradeId != "exact" {
		t.Fatalf("exact priority lost during settlement: handled=%v settled=%+v err=%v", handled, settled, err)
	}
	if handled, settled, err := SettleLegacyObservedPaymentWithResult(reviewPayment(now)); err != nil || !handled || settled != nil {
		t.Fatalf("replay settled again: handled=%v settled=%+v err=%v", handled, settled, err)
	}
	if reloadCollection(t, "other").Status != mdb.StatusWaitPay {
		t.Fatal("fallback candidate changed")
	}
}

func TestReviewCompletedLegacyWindowMustEndAtRelease(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	old := reviewLegacy(t, "completed", mdb.StatusPaySuccess, 5, now.Add(-2*time.Minute), now.Add(time.Minute), "")
	if err := dao.Mdb.Model(old).Updates(map[string]interface{}{"released_at_ms": now.Add(-40 * time.Second).UnixMilli(), "block_transaction_id": "previous-payment"}).Error; err != nil {
		t.Fatal(err)
	}
	reviewLegacy(t, "current", mdb.StatusWaitPay, 5, now.Add(-30*time.Second), now.Add(time.Minute), "")
	if err := LockTransaction("tron", "wallet", "USDT", "current", 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	handled, settled, err := SettleLegacyObservedPaymentWithResult(reviewPayment(now))
	if err != nil || !handled || settled == nil || settled.TradeId != "current" {
		t.Fatalf("released history blocked current owner: handled=%v settled=%+v err=%v", handled, settled, err)
	}
}

func TestReviewManualExclusiveCloseAfterDeadlineMustRemainClosed(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	o := collectionOrder(t, "manual", []mdb.WalletAddress{{Address: "wallet"}}, "")
	now := time.Now()
	if err := dao.Mdb.Model(o).Updates(map[string]interface{}{"assigned_at_ms": now.Add(-time.Minute).UnixMilli(), "payment_expires_at": now.Add(-time.Second)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := CloseExclusivePayment(o.TradeId); err != nil {
		t.Fatal(err)
	}
	p := reviewPayment(now.Add(-2 * time.Second))
	p.AssetID = o.AssetID
	if _, err := ProcessExclusivePayment(p); err != nil {
		t.Fatal(err)
	}
	if got := reloadCollection(t, o.TradeId); got.Status != mdb.StatusExpired {
		t.Fatalf("explicit operator close was reopened: status=%d", got.Status)
	}
}

func TestReviewLegacyManualCloseSurvivesExpirationConfigChange(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	o := reviewLegacy(t, "manual", mdb.StatusWaitPay, 5, now.Add(-3*time.Minute), now.Add(7*time.Minute), "")
	if err := dao.Mdb.Model(o).Update("payment_expires_at", nil).Error; err != nil {
		t.Fatal(err)
	}
	if ok, err := CloseOrderManually(o.TradeId); !ok || err != nil {
		t.Fatalf("close=%v %v", ok, err)
	}
	viper.Set("order_expiration_time", 1)
	if _, _, err := SettleLegacyObservedPaymentWithResult(reviewPayment(now.Add(-150 * time.Second))); err != nil {
		t.Fatal(err)
	}
	if got := reloadCollection(t, o.TradeId); got.Status != mdb.StatusExpired {
		t.Fatalf("changing expiration configuration reopened an explicitly cancelled legacy order: status=%d", got.Status)
	}
}

func TestReviewUnknownClosedHistoryIsNeverAutoReopened(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	order := reviewLegacy(t, "unknown", mdb.StatusExpired, 5, now.Add(-2*time.Minute), now.Add(-time.Minute), "")
	if _, settled, err := SettleLegacyObservedPaymentWithResult(reviewPayment(now.Add(-90 * time.Second))); err != nil || settled != nil {
		t.Fatalf("unknown closure recovered: settled=%+v err=%v", settled, err)
	}
	if reloadCollection(t, order.TradeId).Status != mdb.StatusExpired {
		t.Fatal("unknown closure reopened")
	}
}

func TestReviewLegacyDeadlineSnapshotSurvivesConfigChange(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	viper.Set("order_expiration_time", 10)
	o := &mdb.Orders{TradeId: "snapshot", OrderId: "snapshot", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay}
	o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(time.Now().Add(-2 * time.Minute)))
	if err := LockTransaction(o.Network, o.ReceiveAddress, o.Token, o.TradeId, o.ActualAmount, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := CreateOrderWithTransaction(dao.Mdb, o); err != nil {
		t.Fatal(err)
	}
	deadline := *o.PaymentExpiresAt
	viper.Set("order_expiration_time", 1)
	if !PaymentDeadline(o).Equal(deadline) {
		t.Fatal("deadline changed with configuration")
	}
	if expired, err := UpdateOrderIsExpirationById(o.ID, time.Now().Add(-time.Minute), true); err != nil || expired {
		t.Fatalf("snapshot ignored: expired=%v err=%v", expired, err)
	}
	if err := dao.Mdb.Model(o).Update("payment_expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	if expired, err := UpdateOrderIsExpirationById(o.ID, time.Now().Add(-time.Hour), true); err != nil || !expired {
		t.Fatalf("explicit deadline ignored: expired=%v err=%v", expired, err)
	}
	got := reloadCollection(t, o.TradeId)
	if got.ClosedReason != mdb.OrderClosedExpired || got.PaymentExpiresAt == nil {
		t.Fatal("natural expiry provenance not persisted")
	}
}

func TestReviewMissingLegacyLockStillSettlesWithReceipt(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	o := reviewLegacy(t, "missing-lock", mdb.StatusWaitPay, 5, now.Add(-time.Minute), now.Add(time.Minute), "")
	p := reviewPayment(now)
	for i := 0; i < 2; i++ {
		handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
		if err != nil || !handled || (i == 0 && settled == nil) || (i == 1 && settled != nil) {
			t.Fatalf("retry %d: %v %+v %v", i, handled, settled, err)
		}
	}
	if reloadCollection(t, o.TradeId).Status != mdb.StatusPaySuccess {
		t.Fatal("pending payment lost with expired lock")
	}
}

func TestReviewRecoveryClosesHostedSiblingWithoutResettingPaidProvider(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	root := reviewLegacy(t, "root", mdb.StatusWaitPay, 5, now.Add(-time.Minute), now.Add(time.Minute), "")
	child := &mdb.Orders{TradeId: "hosted", OrderId: "hosted", ParentTradeId: root.TradeId, Status: mdb.StatusWaitPay, PayProvider: mdb.PaymentProviderOkPay}
	if err := dao.Mdb.Create(child).Error; err != nil {
		t.Fatal(err)
	}
	provider := &mdb.ProviderOrder{TradeId: child.TradeId, Provider: child.PayProvider, Status: mdb.ProviderOrderStatusPending}
	if err := dao.Mdb.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	if handled, settled, err := SettleLegacyObservedPaymentWithResult(reviewPayment(now)); err != nil || !handled || settled == nil {
		t.Fatalf("settlement failed: %v %+v %v", handled, settled, err)
	}
	if got := reloadCollection(t, child.TradeId); got.Status != mdb.StatusExpired || got.ClosedReason != mdb.OrderClosedSuperseded {
		t.Fatalf("hosted sibling not retired: %+v", got)
	}
	var got mdb.ProviderOrder
	if err := dao.Mdb.First(&got, provider.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Status != mdb.ProviderOrderStatusExpired {
		t.Fatalf("hosted provider remained selectable: %s", got.Status)
	}
}

func TestReviewTinyEpayPaymentNeverReportsFullFiatAmount(t *testing.T) {
	for _, status := range []int{mdb.StatusWaitPay, mdb.StatusExpired} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			now := time.Now()
			expires := now.Add(time.Minute)
			if status == mdb.StatusExpired {
				expires = now.Add(-time.Minute)
			}
			o := reviewLegacy(t, "tiny", status, 5, now.Add(-3*time.Minute), expires, "https://merchant.test/epay/notify")
			reason := ""
			if status == mdb.StatusExpired {
				reason = mdb.OrderClosedExpired
			}
			if err := dao.Mdb.Model(o).Updates(map[string]interface{}{"token": "TRX", "closed_reason": reason}).Error; err != nil {
				t.Fatal(err)
			}
			p := reviewPayment(now.Add(-2 * time.Minute))
			p.Token = "TRX"
			p.AssetID = ""
			p.RawAmount = big.NewInt(1)
			handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
			if err != nil || !handled || settled != nil {
				t.Fatalf("tiny payment settled: %v %+v %v", handled, settled, err)
			}
			if got := reloadCollection(t, o.TradeId); got.Status != status || got.BlockTransactionId != "" {
				t.Fatalf("tiny payment credited full fiat: %+v", got)
			}
			var receipt mdb.PaymentReceipt
			if err := dao.Mdb.First(&receipt).Error; err != nil {
				t.Fatal(err)
			}
			if receipt.Reason != "below_settlement_precision" {
				t.Fatalf("reason=%s", receipt.Reason)
			}
		})
	}
}

func TestReviewPendingLegacyPaymentCannotBypassSecondBoundary(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	second := now.Truncate(time.Second)
	o := reviewLegacy(t, "boundary", mdb.StatusWaitPay, 5, now.Add(-time.Minute), now.Add(time.Minute), "")
	if err := dao.Mdb.Model(o).Updates(map[string]interface{}{"network": mdb.NetworkSolana, "token": "SOL", "assigned_at_ms": second.Add(500 * time.Millisecond).UnixMilli()}).Error; err != nil {
		t.Fatal(err)
	}
	p := reviewPayment(second)
	p.Network = mdb.NetworkSolana
	p.Token = "SOL"
	p.AssetID = ""
	p.Decimals = 9
	p.RawAmount = big.NewInt(5_000_000_000)
	handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
	if err != nil || !handled || settled != nil {
		t.Fatalf("boundary payment bypassed review: %v %+v %v", handled, settled, err)
	}
	var receipt mdb.PaymentReceipt
	if err := dao.Mdb.First(&receipt).Error; err != nil {
		t.Fatal(err)
	}
	if receipt.Reason != "ambiguous_chain_time" || reloadCollection(t, o.TradeId).Status != mdb.StatusWaitPay {
		t.Fatalf("ambiguous time credited: %+v", receipt)
	}
}

func TestReviewNewLegacyOrderStartsWithFutureDeadline(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	o := &mdb.Orders{TradeId: "new", OrderId: "new", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay}
	if err := LockTransaction(o.Network, o.ReceiveAddress, o.Token, o.TradeId, o.ActualAmount, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := CreateOrderWithTransaction(dao.Mdb, o); err != nil {
		t.Fatal(err)
	}
	got := reloadCollection(t, o.TradeId)
	if got.PaymentExpiresAt == nil || !got.PaymentExpiresAt.After(time.Now().Add(9*time.Minute)) || got.CreatedAt.StdTime().IsZero() {
		t.Fatalf("new order immediately expired: %+v", got)
	}
	if expired, err := UpdateOrderIsExpirationById(o.ID, time.Now().Add(-time.Minute), true); err != nil || expired {
		t.Fatalf("fresh order expired: %v %v", expired, err)
	}
}

func TestReviewTonFriendlyLegacyHistoryRecoversFromRawChainAddress(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	parsed, err := addressutil.ParseTonMainnetAddress("EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	o := reviewLegacy(t, "ton-history", mdb.StatusExpired, 5, now.Add(-3*time.Minute), now.Add(-time.Minute), "")
	if err := dao.Mdb.Model(o).Updates(map[string]interface{}{"network": mdb.NetworkTon, "token": "TON", "receive_address": parsed.Display, "closed_reason": mdb.OrderClosedExpired}).Error; err != nil {
		t.Fatal(err)
	}
	p := reviewPayment(now.Add(-2 * time.Minute))
	p.Network = mdb.NetworkTon
	p.Address = parsed.Raw
	p.Token = "TON"
	p.AssetID = ""
	p.Decimals = 9
	p.RawAmount = big.NewInt(5_000_000_000)
	handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
	if err != nil || !handled || settled == nil || settled.TradeId != o.TradeId {
		t.Fatalf("friendly/raw identity lost payment: %v %+v %v", handled, settled, err)
	}
}

func TestReviewLegacyDeadlineRefreshPreservesOriginalPaymentOwnership(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	o := reviewLegacy(t, "refreshed", mdb.StatusWaitPay, 5, now.Add(-30*time.Minute), now.Add(time.Minute), "")
	if err := LockTransaction(o.Network, o.ReceiveAddress, o.Token, o.TradeId, o.ActualAmount, time.Minute); err != nil {
		t.Fatal(err)
	}
	assigned := reloadCollection(t, o.TradeId).CreatedAt.TimestampMilli()
	for i := 0; i < 2; i++ {
		if err := RefreshOrderExpiration(o.TradeId); err != nil {
			t.Fatal(err)
		}
	}
	got := reloadCollection(t, o.TradeId)
	if got.AssignedAtMs != assigned {
		t.Fatalf("refresh rewrote historical ownership: assigned=%d want=%d", got.AssignedAtMs, assigned)
	}
	start, err := PaymentScanStartTime(o.Network, o.ReceiveAddress, now)
	if err != nil || start.UnixMilli() != assigned {
		t.Fatalf("refreshed order lost initial scan boundary: %v %v", start, err)
	}
	handled, settled, err := SettleLegacyObservedPaymentWithResult(reviewPayment(now.Add(-20 * time.Minute)))
	if err != nil || !handled || settled == nil || settled.TradeId != o.TradeId {
		t.Fatalf("refresh lost original-address payment: %v %+v %v", handled, settled, err)
	}
}
