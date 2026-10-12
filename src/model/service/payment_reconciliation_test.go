package service

import (
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/dromara/carbon/v2"
	"github.com/spf13/viper"
)

func TestUpgradePreservesPayableProviderChild(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	const shopID, shopToken = "test-shop", "test-token"
	if err := data.SetSetting(mdb.SettingGroupOkPay, mdb.SettingKeyOkPayShopID, shopID, mdb.SettingTypeString); err != nil {
		t.Fatal(err)
	}
	if err := data.SetSetting(mdb.SettingGroupOkPay, mdb.SettingKeyOkPayShopToken, shopToken, mdb.SettingTypeString); err != nil {
		t.Fatal(err)
	}
	expiry := time.Now().Add(8 * time.Minute)
	root := mdb.Orders{TradeId: "upgrade-root", OrderId: "upgrade-root", Amount: 35, ActualAmount: 5, Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Status: mdb.StatusWaitPay, PaymentExpiresAt: &expiry}
	root.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(time.Now().Add(-2 * time.Minute)))
	if err := dao.Mdb.Create(&root).Error; err != nil {
		t.Fatal(err)
	}
	child := mdb.Orders{TradeId: "provider-child", OrderId: "provider-child", ParentTradeId: root.TradeId, Amount: 35, ActualAmount: 5, Network: "okpay", Token: "USDT", ReceiveAddress: "OKPAY", PayProvider: mdb.PaymentProviderOkPay, Status: mdb.StatusWaitPay, PaymentExpiresAt: &expiry}
	if err := dao.Mdb.Create(&child).Error; err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Create(&mdb.ProviderOrder{TradeId: child.TradeId, Provider: mdb.PaymentProviderOkPay, ProviderOrderID: "provider-tx", Amount: 5, Coin: "USDT", Status: mdb.ProviderOrderStatusPending}).Error; err != nil {
		t.Fatal(err)
	}
	// The old parent reservation expired after a channel switch. Upgrade sees
	// an empty runtime DB but a still-payable independent provider child.
	viper.Set("runtime_sqlite_filename", filepath.Join(t.TempDir(), "upgrade-runtime.db"))
	if err := dao.RuntimeInit(); err != nil {
		t.Fatal(err)
	}
	defer func() { sqlDB, _ := dao.RuntimeDB.DB(); sqlDB.Close() }()
	got, _ := data.GetOrderInfoByTradeId(root.TradeId)
	if got.ClosedReason != mdb.OrderClosedReservationUnknown {
		t.Fatalf("upgrade state not reproduced: %+v", got)
	}
	form := okPayNotifyTestForm(shopID, shopToken, "provider-tx", child.TradeId, "5.00000000", "USDT")
	if err := HandleOkPayNotify(form, "test callback"); err != nil {
		t.Fatalf("valid unexpired provider payment blocked after upgrade: %v", err)
	}
	got, _ = data.GetOrderInfoByTradeId(root.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.PayBySubId != child.ID || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("root not settled: %+v", got)
	}
	if err := data.ResetCallbackConfirmOk(root.TradeId); err != nil {
		t.Fatal(err)
	}
	if err := HandleOkPayNotify(form, "replayed callback"); err != nil {
		t.Fatal(err)
	}
	got, _ = data.GetOrderInfoByTradeId(root.TradeId)
	if got.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatal("replay queued a second success callback")
	}
	provider, err := data.GetProviderOrderByTradeIDAndProvider(child.TradeId, mdb.PaymentProviderOkPay)
	if err != nil || provider.Status != mdb.ProviderOrderStatusPaid {
		t.Fatalf("provider not paid: %+v %v", provider, err)
	}
}

func TestLateLegacyPaymentLeavesExceptionReceipt(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	deadline := time.Now().Add(-time.Minute)
	order := mdb.Orders{TradeId: "late-legacy", OrderId: "late-legacy", Amount: 35, ActualAmount: 5, Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Status: mdb.StatusExpired, ClosedReason: mdb.OrderClosedExpired, PaymentExpiresAt: &deadline, AssignedAtMs: time.Now().Add(-11 * time.Minute).UnixMilli(), ReleasedAtMs: deadline.UnixMilli()}
	if err := dao.Mdb.Create(&order).Error; err != nil {
		t.Fatal(err)
	}
	token, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkTron, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UnixMilli()
	if err := ProcessTronTRC20Transfer(*token, order.ReceiveAddress, big.NewInt(5000000), "late-tx", stamp); err != nil {
		t.Fatal(err)
	}
	var receipts []mdb.PaymentReceipt
	if err := dao.Mdb.Where("tx_hash = ?", "late-tx").Find(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	got, _ := data.GetOrderInfoByTradeId(order.TradeId)
	if got.Status != mdb.StatusExpired {
		t.Fatal("late payment must not auto-settle")
	}
	if err := ProcessTronTRC20Transfer(*token, order.ReceiveAddress, big.NewInt(5000000), "late-tx", stamp); err != nil {
		t.Fatal(err)
	}
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if count != 1 {
		t.Fatalf("late receipt duplicated: %d", count)
	}
	if len(receipts) != 1 || receipts[0].Status != "exception" || receipts[0].TradeID != "" || receipts[0].ParentTradeID != "" {
		t.Fatalf("scanner acknowledged late funds with no exception: receipts=%+v order_status=%d", receipts, got.Status)
	}
}

// A retired legacy target is distinct from a manually cancelled or already paid
// merchant order. Only the authenticated, still-live provider child may win.
func TestRetiredParentProviderSettlementBoundaries(t *testing.T) {
	for _, scenario := range []string{"manual-parent", "paid-parent", "expired-child", "failed-provider", "wrong-provider-id", "wrong-amount", "bad-signature", "ordinary-entrypoint", "parent-write-failure"} {
		t.Run(scenario, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			if err := data.SetSetting(mdb.SettingGroupOkPay, mdb.SettingKeyOkPayShopID, "shop", mdb.SettingTypeString); err != nil {
				t.Fatal(err)
			}
			if err := data.SetSetting(mdb.SettingGroupOkPay, mdb.SettingKeyOkPayShopToken, "secret", mdb.SettingTypeString); err != nil {
				t.Fatal(err)
			}
			expiry := time.Now().Add(time.Minute)
			root := mdb.Orders{TradeId: "retired", OrderId: "retired", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Amount: 35, ActualAmount: 5, Status: mdb.StatusExpired, ClosedReason: mdb.OrderClosedReservationUnknown, CallBackConfirm: mdb.CallBackConfirmOk}
			child := mdb.Orders{TradeId: "child", OrderId: "child", ParentTradeId: root.TradeId, Network: "okpay", Token: "USDT", ReceiveAddress: "OKPAY", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PayProvider: mdb.PaymentProviderOkPay, PaymentExpiresAt: &expiry, CallBackConfirm: mdb.CallBackConfirmOk}
			provider := mdb.ProviderOrder{TradeId: child.TradeId, Provider: mdb.PaymentProviderOkPay, ProviderOrderID: "provider-tx", Amount: 5, Coin: "USDT", Status: mdb.ProviderOrderStatusPending}
			switch scenario {
			case "manual-parent":
				root.ClosedReason = mdb.OrderClosedManual
			case "paid-parent":
				root.Status = mdb.StatusPaySuccess
				root.PayBySubId = 999
			case "expired-child":
				expiry = time.Now().Add(-time.Second)
			case "failed-provider":
				provider.Status = mdb.ProviderOrderStatusFailed
			}
			for _, row := range []interface{}{&root, &child, &provider} {
				if err := dao.Mdb.Create(row).Error; err != nil {
					t.Fatal(err)
				}
			}
			providerID, amount := "provider-tx", "5"
			if scenario == "wrong-provider-id" {
				providerID = "unrelated-provider-tx"
			}
			if scenario == "wrong-amount" {
				amount = "4"
			}
			form := okPayNotifyTestForm("shop", "secret", providerID, child.TradeId, amount, "USDT")
			if scenario == "bad-signature" {
				form["sign"] = "invalid"
			}
			if scenario == "parent-write-failure" {
				if err := dao.Mdb.Exec(`CREATE TRIGGER reject_provider_parent BEFORE UPDATE ON orders WHEN OLD.trade_id = 'retired' AND NEW.status = 2 BEGIN SELECT RAISE(ABORT, 'injected parent failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			}
			var err error
			if scenario == "ordinary-entrypoint" {
				err = OrderProcessing(&request.OrderProcessingRequest{TradeId: child.TradeId, Amount: 5, BlockTransactionId: "provider-tx"})
			} else {
				err = HandleOkPayNotify(form, "boundary callback")
			}
			if err == nil {
				t.Fatal("invalid recovery unexpectedly succeeded")
			}
			gotRoot, _ := data.GetOrderInfoByTradeId(root.TradeId)
			gotChild, _ := data.GetOrderInfoByTradeId(child.TradeId)
			if gotRoot.Status != root.Status || gotRoot.CallBackConfirm != root.CallBackConfirm || gotChild.Status != child.Status || gotChild.BlockTransactionId != "" {
				t.Fatalf("failed settlement changed group: root=%+v child=%+v", gotRoot, gotChild)
			}
			if scenario == "parent-write-failure" {
				if err := dao.Mdb.Exec("DROP TRIGGER reject_provider_parent").Error; err != nil {
					t.Fatal(err)
				}
				if err := HandleOkPayNotify(form, "retry after rollback"); err != nil {
					t.Fatal(err)
				}
				gotRoot, _ = data.GetOrderInfoByTradeId(root.TradeId)
				if gotRoot.Status != mdb.StatusPaySuccess {
					t.Fatal("retry failed to settle")
				}
			}
		})
	}
}

func TestUnassignedLegacyPaymentPreservesReusedReservation(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	deadline := now.Add(-time.Minute)
	old := mdb.Orders{TradeId: "old", OrderId: "old", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Amount: 35, ActualAmount: 5, Status: mdb.StatusExpired, ClosedReason: mdb.OrderClosedExpired, AssignedAtMs: now.Add(-11 * time.Minute).UnixMilli(), PaymentExpiresAt: &deadline, ReleasedAtMs: deadline.UnixMilli(), CallBackConfirm: mdb.CallBackConfirmOk}
	if err := dao.Mdb.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.LockTransaction("tron", "wallet", "USDT", "new", 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	fresh := mdb.Orders{TradeId: "new", OrderId: "new", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, AssignedAtMs: now.Add(-10 * time.Second).UnixMilli()}
	if err := data.CreateOrderWithTransaction(dao.Mdb, &fresh); err != nil {
		t.Fatal(err)
	}
	token, err := data.GetEnabledChainTokenBySymbol("tron", "USDT")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := ProcessTronTRC20Transfer(*token, "wallet", big.NewInt(5000000), "gap-payment", now.Add(-30*time.Second).UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	gotOld, _ := data.GetOrderInfoByTradeId("old")
	gotNew, _ := data.GetOrderInfoByTradeId("new")
	if gotOld.Status != mdb.StatusExpired || gotOld.CallBackConfirm != mdb.CallBackConfirmOk || gotNew.Status != mdb.StatusWaitPay {
		t.Fatal("unassigned funds changed orders or callbacks")
	}
	if id, err := data.GetTradeIdByWalletAddressAndAmountAndToken("tron", "wallet", "USDT", 5); err != nil || id != "new" {
		t.Fatalf("new reservation lost: %s %v", id, err)
	}
	var receipt mdb.PaymentReceipt
	if err := dao.Mdb.Where("tx_hash = ?", "gap-payment").First(&receipt).Error; err != nil || receipt.TradeID != "" || receipt.ParentTradeID != "" || receipt.Reason != "unassigned" {
		t.Fatalf("guessed owner for gap payment: %+v %v", receipt, err)
	}
	if err := ProcessTronTRC20Transfer(*token, "wallet", big.NewInt(5000000), "new-payment", now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	gotNew, _ = data.GetOrderInfoByTradeId("new")
	if gotNew.Status != mdb.StatusPaySuccess {
		t.Fatal("exception blocked valid new payment")
	}
}

func TestUnassignedLegacyReceiptWriteFailureIsRetryable(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	deadline := time.Now().Add(-time.Minute)
	old := mdb.Orders{TradeId: "old", OrderId: "old", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Amount: 35, ActualAmount: 5, Status: mdb.StatusExpired, ClosedReason: mdb.OrderClosedExpired, AssignedAtMs: deadline.Add(-10 * time.Minute).UnixMilli(), PaymentExpiresAt: &deadline, ReleasedAtMs: deadline.UnixMilli()}
	if err := dao.Mdb.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	token, err := data.GetEnabledChainTokenBySymbol("tron", "USDT")
	if err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Exec(`CREATE TRIGGER reject_unassigned_receipt BEFORE INSERT ON payment_receipt BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UnixMilli()
	scan := func() error {
		return ProcessTronTRC20Transfer(*token, "wallet", big.NewInt(5000000), "retry-payment", stamp)
	}
	if err := scan(); err == nil {
		t.Fatal("scanner acknowledged payment whose receipt was not durable")
	}
	if err := dao.Mdb.Exec("DROP TRIGGER reject_unassigned_receipt").Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := scan(); err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("retry lost/duplicated funds: %d %v", count, err)
	}
}
