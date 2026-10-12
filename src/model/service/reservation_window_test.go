package service

import (
	"context"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/dromara/carbon/v2"
	"gorm.io/gorm"
)

func TestNewOrderReservationCoversDeadline(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := data.SetSetting("rate", "rate.forced_rate_list", `{"cny":{"usdt":0.1}}`, "json"); err != nil {
		t.Fatal(err)
	}
	if _, err := data.AddWalletAddress("TTestTronAddress001"); err != nil {
		t.Fatal(err)
	}
	// The actual create service reserves first, then snapshots precision from
	// runtime DB. Delay that read to model ordinary DB contention before insert.
	hook := "review3_slow_reservation_read"
	if err := dao.RuntimeDB.Callback().Query().After("gorm:query").Register(hook, func(tx *gorm.DB) {
		if lock, ok := tx.Statement.Dest.(*mdb.TransactionLock); ok && lock.ID != 0 {
			time.Sleep(time.Second)
		}
	}); err != nil {
		t.Fatal(err)
	}
	created, err := CreateTransaction(newCreateTransactionRequest("old", 50), nil)
	dao.RuntimeDB.Callback().Query().Remove(hook)
	if err != nil {
		t.Fatal(err)
	}
	old, err := data.GetOrderInfoByTradeId(created.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	var lock mdb.TransactionLock
	if err := dao.RuntimeDB.Where("trade_id = ?", old.TradeId).First(&lock).Error; err != nil {
		t.Fatal(err)
	}
	if data.PaymentDeadline(old).After(lock.ExpiresAt) {
		t.Fatal("business deadline exceeds reservation")
	}
	// Advance the original order and reservation together to the lock's expiry,
	// leaving the independently calculated business deadline unchanged relative to it.
	shift := lock.ExpiresAt.Sub(time.Now().Add(-100 * time.Millisecond))
	if err := dao.RuntimeDB.Model(&lock).Update("expires_at", lock.ExpiresAt.Add(-shift)).Error; err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(old).Updates(map[string]interface{}{"created_at": old.CreatedAt.StdTime().Add(-shift), "assigned_at_ms": old.AssignedAtMs - shift.Milliseconds(), "payment_expires_at": data.PaymentDeadline(old).Add(-shift)}).Error; err != nil {
		t.Fatal(err)
	}
	created, err = CreateTransaction(newCreateTransactionRequest("new", 50), nil)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := data.GetOrderInfoByTradeId(created.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ActualAmount != old.ActualAmount || fresh.ReceiveAddress != old.ReceiveAddress {
		t.Fatal("fixture did not reuse reservation")
	}
	time.Sleep(10 * time.Millisecond)
	p := data.ObservedPayment{Network: "tron", Address: fresh.ReceiveAddress, Token: "USDT", AssetID: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", RawAmount: big.NewInt(5000000), Decimals: 6, TxHash: "new-payment", EventID: "contract:0", BlockTimeMs: time.Now().UnixMilli()}
	handled, settled, err := data.SettleLegacyObservedPaymentWithResult(p)
	if err != nil {
		t.Fatal(err)
	}
	var receipt mdb.PaymentReceipt
	dao.Mdb.First(&receipt)
	if !handled || settled == nil || settled.TradeId != fresh.TradeId {
		t.Fatalf("new order payment blocked: handled=%v settled=%+v reason=%s", handled, settled, receipt.Reason)
	}
}

func TestLegacySuccessorOverridesMissingReservationHistory(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	// Old deployed code refreshes created_at when changing channels but leaves
	// the runtime reservation at its original deadline. This is real upgrade state.
	old := &mdb.Orders{TradeId: "old-upgrade", OrderId: "old-upgrade", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", ActualAmount: 5, Amount: 35, Status: mdb.StatusWaitPay}
	old.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-2 * time.Minute)))
	if err := dao.Mdb.Create(old).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.LockTransaction("tron", "wallet", "USDT", old.TradeId, 5, -time.Minute); err != nil {
		t.Fatal(err)
	}
	dao.ResetMdbTableInitForTest()
	t.Cleanup(dao.ResetMdbTableInitForTest)
	dao.MdbTableInit()
	if err := data.LockTransaction("tron", "wallet", "USDT", "new-upgrade", 5, config.GetOrderExpirationTimeDuration()); err != nil {
		t.Fatal(err)
	}
	fresh := &mdb.Orders{TradeId: "new-upgrade", OrderId: "new-upgrade", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", ActualAmount: 5, Amount: 35, Status: mdb.StatusWaitPay}
	if err := data.CreateOrderWithTransaction(dao.Mdb, fresh); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	p := data.ObservedPayment{Network: "tron", Address: "wallet", Token: "USDT", AssetID: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", RawAmount: big.NewInt(5000000), Decimals: 6, TxHash: "upgrade-new-payment", EventID: "contract:0", BlockTimeMs: time.Now().UnixMilli()}
	handled, settled, err := data.SettleLegacyObservedPaymentWithResult(p)
	if err != nil {
		t.Fatal(err)
	}
	var receipt mdb.PaymentReceipt
	dao.Mdb.First(&receipt)
	if !handled || settled == nil || settled.TradeId != fresh.TradeId {
		t.Fatalf("upgrade blocked valid reused reservation: handled=%v settled=%+v receipt=%s", handled, settled, receipt.Reason)
	}
}

func TestPaidSolanaCancellationDoesNotWaitForOrdinaryRPC(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":[]}`))
	}))
	defer server.Close()
	t.Cleanup(setupSolanaRPCNode(t, server.URL))
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	if err := dao.Mdb.Model(&mdb.RpcNode{}).Where("network = ?", mdb.NetworkSolana).Update("purpose", mdb.RpcNodePurposePaidFallback).Error; err != nil {
		t.Fatal(err)
	}
	ordinary := make(chan error, 1)
	go func() {
		_, err := solRetryClientValidatedWithContext(context.Background(), "getSignaturesForAddress", []interface{}{"owner"}, nil)
		ordinary <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("ordinary RPC did not start")
	}
	ctx, cancel := context.WithTimeout(WithPaidPaymentReview(context.Background()), 50*time.Millisecond)
	defer cancel()
	review := make(chan error, 1)
	go func() {
		_, err := solRetryClientValidatedWithContext(ctx, "getSignaturesForAddress", []interface{}{"owner"}, nil)
		review <- err
	}()
	<-ctx.Done()
	var blocked bool
	var err error
	select {
	case err = <-review:
	case <-time.After(200 * time.Millisecond):
		blocked = true
	}
	close(release)
	if blocked {
		err = <-review
	}
	if normalErr := <-ordinary; normalErr != nil {
		t.Fatal(normalErr)
	}
	if blocked || err == nil {
		t.Fatalf("paid review ignored cancelled budget: blockedBeyondDeadline=%v returnedErr=%v", blocked, err)
	}
}
