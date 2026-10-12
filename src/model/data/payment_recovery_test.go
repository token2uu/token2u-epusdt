package data

import (
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/dromara/carbon/v2"
)

func TestHistoricalPaymentRecoversOriginalLegacyOrder(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now().Truncate(time.Second)
	expires := now.Add(-time.Minute)
	old := &mdb.Orders{TradeId: "old", OrderId: "old", Network: "tron", Token: "USDT", ReceiveAddress: "wallet",
		ClosedReason: mdb.OrderClosedExpired, Status: mdb.StatusExpired, Amount: 35, ActualAmount: 5, Currency: "CNY", PaymentExpiresAt: &expires, CallBackConfirm: mdb.CallBackConfirmOk}
	old.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-3 * time.Minute)))
	if err := dao.Mdb.Create(old).Error; err != nil {
		t.Fatal(err)
	}
	newExpiry := now.Add(time.Minute)
	current := &mdb.Orders{TradeId: "new", OrderId: "new", Network: "tron", Token: "USDT", ReceiveAddress: "wallet",
		Status: mdb.StatusWaitPay, Amount: 35, ActualAmount: 5, PaymentExpiresAt: &newExpiry}
	current.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-30 * time.Second)))
	if err := dao.Mdb.Create(current).Error; err != nil {
		t.Fatal(err)
	}
	if err := LockTransaction("tron", "wallet", "USDT", "new", 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	p := ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", AssetID: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", TxHash: "original-payment", EventID: "0",
		RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: now.Add(-2 * time.Minute).UnixMilli()}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handled, _, err := SettleLegacyObservedPaymentWithResult(p)
			if !handled || err != nil {
				t.Errorf("handled=%v err=%v", handled, err)
			}
		}()
	}
	wg.Wait()
	got := reloadCollection(t, "old")
	if got.Status != mdb.StatusPaySuccess || got.BlockTransactionId != p.TxHash || got.CallBackConfirm != mdb.CallBackConfirmNo || got.CallbackNum != 0 {
		t.Fatalf("original order not recovered: %+v", got)
	}
	if fresh := reloadCollection(t, "new"); fresh.Status != mdb.StatusWaitPay || fresh.BlockTransactionId != "" {
		t.Fatalf("new order credited: %+v", fresh)
	}
	if id, err := GetTradeIdByWalletAddressAndAmountAndToken("tron", "wallet", "USDT", 5); err != nil || id != "new" {
		t.Fatalf("new reservation changed: %s %v", id, err)
	}
	var receipts int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != 1 {
		t.Fatalf("dedup count=%d err=%v", receipts, err)
	}
}

func TestHistoricalLegacyAmbiguityRemainsHandledOnReplay(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	for i, status := range []int{mdb.StatusExpired, mdb.StatusWaitPay} {
		expires := now.Add(time.Minute)
		order := &mdb.Orders{TradeId: []string{"closed", "pending"}[i], OrderId: []string{"closed", "pending"}[i], Network: "tron", Token: "USDT", ReceiveAddress: "wallet",
			Status: status, Amount: 35, ActualAmount: 5, PaymentExpiresAt: &expires}
		order.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-time.Minute)))
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
	}
	p := ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", TxHash: "ambiguous", EventID: "0", RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: now.UnixMilli()}
	for i := 0; i < 2; i++ {
		if handled, settled, err := SettleLegacyObservedPaymentWithResult(p); !handled || settled != nil || err != nil {
			t.Fatalf("ambiguous replay fell through to current lock: handled=%v settled=%v err=%v", handled, settled, err)
		}
	}
	var receipts []mdb.PaymentReceipt
	if err := dao.Mdb.Find(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 1 || receipts[0].Reason != "ambiguous_assignment" || reloadCollection(t, "pending").Status != mdb.StatusWaitPay {
		t.Fatalf("ambiguous payment credited: %+v", receipts)
	}
}

func TestClosedSuccessorStillEndsLegacyOwnership(t *testing.T) {
	for _, successorAmount := range []float64{5, 5.001} {
		t.Run(fmt.Sprintf("successor_amount_%g", successorAmount), func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			now := time.Now().Truncate(time.Second)
			deadline := now.Add(time.Minute)
			precision := 2
			old := mdb.Orders{TradeId: "stale-parent", OrderId: "stale-parent", Network: "tron", Token: "USDT", ReceiveAddress: "wallet",
				Status: mdb.StatusWaitPay, Amount: 35, ActualAmount: 5, AmountPrecision: &precision,
				AssignedAtMs: now.Add(-5 * time.Minute).UnixMilli(), PaymentExpiresAt: &deadline}
			if err := dao.Mdb.Create(&old).Error; err != nil {
				t.Fatal(err)
			}
			closedDeadline := now.Add(-2 * time.Minute)
			successor := mdb.Orders{TradeId: "closed-successor", OrderId: "closed-successor", Network: "tron", Token: "USDT", ReceiveAddress: "wallet",
				Status: mdb.StatusExpired, ActualAmount: successorAmount, AssignedAtMs: now.Add(-3 * time.Minute).UnixMilli(),
				PaymentExpiresAt: &closedDeadline, ReleasedAtMs: closedDeadline.UnixMilli(), ClosedReason: mdb.OrderClosedExpired}
			if err := dao.Mdb.Create(&successor).Error; err != nil {
				t.Fatal(err)
			}
			p := ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", AssetID: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", TxHash: "after-successor", EventID: "0",
				RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: now.UnixMilli()}
			handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
			if err != nil {
				t.Fatal(err)
			}
			if successorAmount == 5 {
				if !handled || settled != nil || reloadCollection(t, old.TradeId).Status != mdb.StatusWaitPay {
					t.Fatalf("credited stale reservation after closed successor: handled=%v settled=%+v", handled, settled)
				}
				var receipt mdb.PaymentReceipt
				if err := dao.Mdb.First(&receipt).Error; err != nil || receipt.Status != "exception" || receipt.TradeID != "" {
					t.Fatalf("unassigned funds not retained: %+v %v", receipt, err)
				}
			} else if !handled || settled == nil || settled.TradeId != old.TradeId || reloadCollection(t, old.TradeId).Status != mdb.StatusPaySuccess {
				t.Fatalf("different exact amount incorrectly ended ownership: handled=%v settled=%+v", handled, settled)
			}
		})
	}
}

func TestPaymentReviewIncludesOnlyDueOnChainOrders(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	for _, spec := range []struct {
		trade, provider string
		status          int
		deadline        time.Duration
		cancelled       bool
	}{
		{"near", mdb.PaymentProviderOnChain, mdb.StatusWaitPay, time.Minute, false},
		{"recent-expired", "", mdb.StatusExpired, -time.Minute, false},
		{"far", "", mdb.StatusWaitPay, time.Hour, false},
		{"old", "", mdb.StatusExpired, -time.Hour, false},
		{"cancelled", "", mdb.StatusExpired, time.Minute, true},
		{"hosted", "okpay", mdb.StatusWaitPay, time.Minute, false},
		{"paid", "", mdb.StatusPaySuccess, -time.Minute, false},
	} {
		expires := now.Add(spec.deadline)
		order := &mdb.Orders{TradeId: spec.trade, OrderId: spec.trade, Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Status: spec.status, PayProvider: spec.provider, PaymentExpiresAt: &expires}
		if spec.status == mdb.StatusExpired {
			order.ClosedReason = mdb.OrderClosedExpired
		}
		if spec.cancelled {
			order.ClosedReason = mdb.OrderClosedManual
			order.ReleasedAtMs = now.UnixMilli()
		}
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
	}
	orders, err := ListPaymentReviewOrders("tron", now)
	if err != nil || len(orders) != 2 || orders[0].TradeId != "recent-expired" || orders[1].TradeId != "near" {
		t.Fatalf("incorrect review scope: orders=%+v err=%v", orders, err)
	}
}

func TestExpiredExclusiveChildRecoveryIsAtomicAndPreservesNewOwner(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	root := collectionOrder(t, "root", []mdb.WalletAddress{{Address: "root-wallet"}}, "")
	child := collectionOrder(t, "child", []mdb.WalletAddress{{Address: "child-wallet"}}, root.TradeId)
	now := time.Now()
	paidAt := now.Add(-2 * time.Minute).UnixMilli()
	deadline := now.Add(-time.Minute)
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("trade_id IN ?", []string{"root", "child"}).Updates(map[string]interface{}{"assigned_at_ms": paidAt - 1000, "payment_expires_at": deadline}).Error; err != nil {
		t.Fatal(err)
	}
	if err := ExpireExclusivePayments(); err != nil {
		t.Fatal(err)
	}
	newOrder := collectionOrder(t, "new", []mdb.WalletAddress{{Address: "child-wallet"}}, "")
	p := ObservedPayment{Network: "tron", Address: child.ReceiveAddress, Token: "USDT", AssetID: child.AssetID, TxHash: "historical", EventID: "0", RawAmount: big.NewInt(3_000_000), Decimals: 6, BlockTimeMs: paidAt}
	for i := 0; i < 3; i++ {
		if handled, err := ProcessExclusivePayment(p); !handled || err != nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	}
	gotRoot, gotChild := reloadCollection(t, "root"), reloadCollection(t, "child")
	if gotRoot.Status != mdb.StatusPaySuccess || gotRoot.PayBySubId != child.ID || gotRoot.PaidAmount != 21 || gotRoot.CallBackConfirm != mdb.CallBackConfirmNo || gotChild.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatalf("bad group recovery: root=%+v child=%+v", gotRoot, gotChild)
	}
	var slot mdb.PaymentAddress
	dao.Mdb.Where("address = ?", child.ReceiveAddress).First(&slot)
	if slot.TradeID != newOrder.TradeId || reloadCollection(t, "new").Status != mdb.StatusWaitPay {
		t.Fatal("late release stole new assignment")
	}
}

func TestManualCloseAndPaidSiblingAreNeverReopened(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	order := collectionOrder(t, "cancelled", []mdb.WalletAddress{{Address: "wallet"}}, "")
	paidAt := time.Now().UnixMilli()
	if err := CloseExclusivePayment(order.TradeId); err != nil {
		t.Fatal(err)
	}
	p := ObservedPayment{Network: "tron", Address: order.ReceiveAddress, Token: "USDT", AssetID: order.AssetID, TxHash: "cancelled-tx", EventID: "0", RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: paidAt}
	if _, err := ProcessExclusivePayment(p); err != nil {
		t.Fatal(err)
	}
	if got := reloadCollection(t, order.TradeId); got.Status != mdb.StatusExpired || got.PaidAmount != 0 {
		t.Fatalf("cancelled order revived: %+v", got)
	}
	root := collectionOrder(t, "root", []mdb.WalletAddress{{Address: "a"}}, "")
	child := collectionOrder(t, "child", []mdb.WalletAddress{{Address: "b"}}, root.TradeId)
	observeCollection(t, root, "winner", 5_000_000)
	observeCollection(t, child, "loser", 5_000_000)
	if got := reloadCollection(t, root.TradeId); got.PayBySubId != 0 || got.BlockTransactionId != "winner" {
		t.Fatalf("second winner: %+v", got)
	}
}

func TestStaleClosedCallbackCannotAcknowledgeRecoveredPayment(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	order := collectionOrder(t, "callback", []mdb.WalletAddress{{Address: "wallet"}}, "")
	now := time.Now()
	dao.Mdb.Model(order).Updates(map[string]interface{}{"assigned_at_ms": now.Add(-2 * time.Minute).UnixMilli(), "payment_expires_at": now.Add(-time.Minute)})
	if err := ExpireExclusivePayments(); err != nil {
		t.Fatal(err)
	}
	stale := reloadCollection(t, order.TradeId)
	p := ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", AssetID: order.AssetID, TxHash: "tx", EventID: "0", RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: now.Add(-90 * time.Second).UnixMilli()}
	if _, err := ProcessExclusivePayment(p); err != nil {
		t.Fatal(err)
	}
	stale.CallBackConfirm = mdb.CallBackConfirmOk
	if err := SaveCallBackOrdersResp(&stale); err != nil {
		t.Fatal(err)
	}
	got := reloadCollection(t, order.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.CallBackConfirm != mdb.CallBackConfirmNo || got.CallbackNum != 0 {
		t.Fatalf("stale callback consumed success: %+v", got)
	}
}
