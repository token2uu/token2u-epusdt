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
)

func collectionOrder(t *testing.T, id string, wallets []mdb.WalletAddress, parent string) *mdb.Orders {
	t.Helper()
	expiry := time.Now().Add(10 * time.Minute)
	order := &mdb.Orders{TradeId: id, OrderId: id, Network: "tron", Token: "USDT", Currency: "CNY", Amount: 35, ActualAmount: 5,
		Status: mdb.StatusWaitPay, NotifyUrl: "https://merchant.test/notify", PaymentType: mdb.PaymentTypeEpay, ParentTradeId: parent, PaymentExpiresAt: &expiry}
	if parent != "" {
		order.NotifyUrl = ""
	}
	if err := CreateExclusivePayment(order, mdb.ChainToken{ContractAddress: "usdt-contract", Decimals: 6}, wallets, false); err != nil {
		t.Fatal(err)
	}
	return order
}
func observeCollection(t *testing.T, o *mdb.Orders, hash string, amount int64) {
	t.Helper()
	handled, err := ProcessExclusivePayment(ObservedPayment{Network: o.Network, Address: o.ReceiveAddress, Token: o.Token, AssetID: o.AssetID,
		TxHash: hash, EventID: "transfer:0", RawAmount: big.NewInt(amount), Decimals: 6, BlockTimeMs: time.Now().UnixMilli()})
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}
func reloadCollection(t *testing.T, id string) mdb.Orders {
	t.Helper()
	var o mdb.Orders
	if err := dao.Mdb.Where("trade_id = ?", id).First(&o).Error; err != nil {
		t.Fatal(err)
	}
	return o
}
func TestExclusiveActualSettlementAndDurableDedup(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	for i, amount := range []int64{3_000_000, 6_000_000} {
		o := collectionOrder(t, fmt.Sprintf("order%d", i), []mdb.WalletAddress{{Address: fmt.Sprintf("wallet%d", i)}}, "")
		observeCollection(t, o, fmt.Sprintf("tx%d", i), amount)
		observeCollection(t, o, fmt.Sprintf("tx%d", i), amount)
		got := reloadCollection(t, o.TradeId)
		if got.Status != mdb.StatusPaySuccess || got.ActualAmount != 5 || got.Amount != 35 || got.PaidAmount != float64(amount)/1e6*7 {
			t.Fatalf("unexpected settlement: %+v", got)
		}
		if got.PaidCryptoAmount != fmt.Sprint(amount/1e6) || got.CallBackConfirm != mdb.CallBackConfirmNo {
			t.Fatalf("wrong actual amount or callback: %+v", got)
		}
		var slot mdb.PaymentAddress
		dao.Mdb.Where("address = ?", o.ReceiveAddress).First(&slot)
		if slot.TradeID != "" {
			t.Fatal("address not released")
		}
	}
	var n int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&n)
	if n != 2 {
		t.Fatalf("receipt count=%d", n)
	}
}
func TestExclusiveLateAndWrongAssetNeverComplete(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	o := collectionOrder(t, "late", []mdb.WalletAddress{{Address: "wallet"}}, "")
	expiry := time.Now().Add(-time.Second)
	dao.Mdb.Model(o).Updates(map[string]interface{}{"payment_expires_at": expiry, "assigned_at_ms": expiry.Add(-time.Minute).UnixMilli()})
	observeCollection(t, o, "late-tx", 5_000_000)
	got := reloadCollection(t, o.TradeId)
	if got.Status != mdb.StatusExpired || got.PaidAmount != 0 || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("late settled or close notice missing: %+v", got)
	}
	var receipt mdb.PaymentReceipt
	dao.Mdb.First(&receipt)
	if receipt.Reason != "late_payment" {
		t.Fatalf("reason=%s", receipt.Reason)
	}
	o = collectionOrder(t, "wrong", []mdb.WalletAddress{{Address: "other"}}, "")
	handled, err := ProcessExclusivePayment(ObservedPayment{Network: o.Network, Address: o.ReceiveAddress, Token: o.Token, AssetID: "fake", TxHash: "fake", EventID: "0", RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: time.Now().UnixMilli()})
	if !handled || err != nil || reloadCollection(t, o.TradeId).Status != mdb.StatusWaitPay {
		t.Fatalf("wrong asset settled: %v", err)
	}
}
func TestExclusiveRotationAndOwnership(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	wallets := []mdb.WalletAddress{{BaseModel: mdb.BaseModel{ID: 1}, Address: "a"}, {BaseModel: mdb.BaseModel{ID: 2}, Address: "b"}}
	first := collectionOrder(t, "first", wallets, "")
	observeCollection(t, first, "tx1", 3_000_000)
	second := collectionOrder(t, "second", wallets, "")
	if second.ReceiveAddress == first.ReceiveAddress {
		t.Fatal("unused address not preferred")
	}
	third := collectionOrder(t, "third", wallets, "")
	if third.ReceiveAddress != first.ReceiveAddress {
		t.Fatal("released address not reused")
	}
	// Redelivery of first receipt must not complete third or release its slot.
	observeCollection(t, first, "tx1", 3_000_000)
	if reloadCollection(t, third.TradeId).Status != mdb.StatusWaitPay {
		t.Fatal("replay completed new owner")
	}
	var slot mdb.PaymentAddress
	dao.Mdb.Where("address = ?", third.ReceiveAddress).First(&slot)
	if slot.TradeID != third.TradeId {
		t.Fatal("stale release cleared current owner")
	}
}
func TestExclusiveConcurrentAllocationAndParentWinner(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			exp := time.Now().Add(time.Minute)
			o := &mdb.Orders{TradeId: fmt.Sprint(i), OrderId: fmt.Sprint(i), Network: "tron", Token: "USDT", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentExpiresAt: &exp}
			results <- CreateExclusivePayment(o, mdb.ChainToken{ContractAddress: "usdt-contract", Decimals: 6}, []mdb.WalletAddress{{Address: "single"}}, false)
		}(i)
	}
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		if err == nil {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("%d allocated same slot", won)
	}
	root := collectionOrder(t, "root", []mdb.WalletAddress{{Address: "parent"}}, "")
	child := collectionOrder(t, "child", []mdb.WalletAddress{{Address: "child"}}, root.TradeId)
	observeCollection(t, child, "child-tx", 6_000_000)
	observeCollection(t, root, "parent-tx", 3_000_000)
	got := reloadCollection(t, root.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.PaidAmount != 42 || got.PayBySubId != child.ID {
		t.Fatalf("wrong winner: %+v", got)
	}
	var occupied int64
	dao.Mdb.Model(&mdb.PaymentAddress{}).Where("trade_id IN ?", []string{root.TradeId, child.TradeId}).Count(&occupied)
	if occupied != 0 {
		t.Fatal("family addresses not released")
	}
	var settled int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("status = ?", "settled").Count(&settled)
	if settled != 1 {
		t.Fatal("family settled twice")
	}
}
func TestExclusiveLegacyLockBlocksAllocation(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	if err := LockTransaction("tron", "legacy", "USDT", "old", 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	o := collectionOrder(t, "new", []mdb.WalletAddress{{Address: "legacy"}, {Address: "free"}}, "")
	if o.ReceiveAddress != "free" {
		t.Fatal("overlaid legacy lock")
	}
}

func TestExclusiveHostedProviderReleasesParentAddress(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	root := collectionOrder(t, "hosted-root", []mdb.WalletAddress{{Address: "root-wallet"}}, "")
	child := mdb.Orders{TradeId: "hosted-child", OrderId: "hosted-child", ParentTradeId: root.TradeId, CollectionMode: mdb.CollectionExclusive, PaymentExpiresAt: root.PaymentExpiresAt, PayProvider: mdb.PaymentProviderOkPay, Status: mdb.StatusWaitPay, Amount: 35, ActualAmount: 5, CallBackConfirm: mdb.CallBackConfirmOk}
	if err := dao.Mdb.Create(&child).Error; err != nil {
		t.Fatal(err)
	}
	if err := SettleExclusiveProvider(child.TradeId, "provider-payment"); err != nil {
		t.Fatal(err)
	}
	got := reloadCollection(t, root.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.PayBySubId != child.ID || got.PaidAmount != 35 {
		t.Fatalf("provider did not complete root: %+v", got)
	}
	var slots int64
	dao.Mdb.Model(&mdb.PaymentAddress{}).Where("trade_id = ?", root.TradeId).Count(&slots)
	if slots != 0 {
		t.Fatal("provider left address occupied")
	}
	observeCollection(t, root, "other-chain-payment", 3_000_000)
	if reloadCollection(t, root.TradeId).PaidAmount != 35 {
		t.Fatal("late sibling overwrote provider payment")
	}
}

func TestExclusiveDoesNotConsumeLegacyOtherAsset(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	o := collectionOrder(t, "exclusive-usdt", []mdb.WalletAddress{{Address: "shared-wallet"}}, "")
	legacy := mdb.Orders{TradeId: "legacy-usdc", OrderId: "legacy-usdc", Network: "tron", ReceiveAddress: o.ReceiveAddress, Token: "USDC", Status: mdb.StatusWaitPay}
	if err := dao.Mdb.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	handled, err := ProcessExclusivePayment(ObservedPayment{Network: "tron", Address: o.ReceiveAddress, Token: "USDC", AssetID: "usdc", TxHash: "usdc-tx", EventID: "0", RawAmount: big.NewInt(3_000_000), Decimals: 6, BlockTimeMs: time.Now().UnixMilli()})
	if handled || err != nil {
		t.Fatalf("legacy asset intercepted: %v %v", handled, err)
	}
}
