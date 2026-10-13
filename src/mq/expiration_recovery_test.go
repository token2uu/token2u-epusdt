package mq

import (
	"fmt"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestExpirationWriteFailuresDoNotBlockLaterOrders(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	var blockedIDs []uint64
	for i := 0; i < batchSize; i++ {
		id := fmt.Sprintf("blocked-expiry-%d", i)
		order := &mdb.Orders{TradeId: id, OrderId: id, Amount: 35, Status: mdb.StatusWaitSelect}
		if err := data.CreateOrderWithTransaction(dao.Mdb, order); err != nil {
			t.Fatal(err)
		}
		blockedIDs = append(blockedIDs, order.ID)
	}
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("id IN ?", blockedIDs).UpdateColumn("payment_expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.LockTransaction(mdb.NetworkTron, "healthy-address", "USDT", "healthy-expiry", 5, time.Hour); err != nil {
		t.Fatal(err)
	}
	healthy := &mdb.Orders{TradeId: "healthy-expiry", OrderId: "healthy-expiry", Amount: 35, ActualAmount: 5,
		Network: mdb.NetworkTron, Token: "USDT", ReceiveAddress: "healthy-address", Status: mdb.StatusWaitPay,
		NotifyUrl: "https://merchant.example/callback", CallBackConfirm: mdb.CallBackConfirmOk}
	if err := data.CreateOrderWithTransaction(dao.Mdb, healthy); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(healthy).UpdateColumn("payment_expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	// Reads still work, but a full oldest batch cannot be updated. This is a
	// persistent storage failure until the trigger is removed during recovery.
	if err := dao.Mdb.Exec(`CREATE TRIGGER reject_expiration BEFORE UPDATE OF status ON orders
		WHEN OLD.trade_id LIKE 'blocked-expiry-%'
		BEGIN SELECT RAISE(ABORT, 'expiration temporarily unavailable'); END`).Error; err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { processExpiredOrders(); close(done) }()
	completed := false
	select {
	case <-done:
		completed = true
	case <-time.After(2 * time.Second):
	}
	// Stop an unfixed implementation's retry loop before cleaning up the DB.
	if err := dao.Mdb.Exec("DROP TRIGGER reject_expiration").Error; err != nil {
		t.Fatal(err)
	}
	if !completed {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("expiration worker did not recover")
		}
		t.Fatal("expiration worker loops on a failed batch instead of finishing the round")
	}
	got, err := data.GetOrderInfoByTradeId(healthy.TradeId)
	if err != nil || got.Status != mdb.StatusExpired || got.ClosedReason != mdb.OrderClosedExpired || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("later healthy order was blocked: %+v %v", got, err)
	}
	var locks int64
	if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", healthy.TradeId).Count(&locks).Error; err != nil || locks != 0 {
		t.Fatalf("healthy order reservation not released: %d %v", locks, err)
	}
	var waiting int64
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("id IN ? AND status = ?", blockedIDs, mdb.StatusWaitSelect).Count(&waiting).Error; err != nil || waiting != batchSize {
		t.Fatalf("failed writes changed order state: %d %v", waiting, err)
	}
	if err := data.ResetCallbackConfirmOk(healthy.TradeId); err != nil {
		t.Fatal(err)
	}
	// A later round starts from the beginning and retries previously failed IDs.
	processExpiredOrders()
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("id IN ? AND status = ? AND closed_reason = ?", blockedIDs, mdb.StatusExpired, mdb.OrderClosedExpired).Count(&waiting).Error; err != nil || waiting != batchSize {
		t.Fatalf("failed orders not retried after storage recovery: %d %v", waiting, err)
	}
	got, err = data.GetOrderInfoByTradeId(healthy.TradeId)
	if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatalf("retry reset completed callback: %+v %v", got, err)
	}
}
