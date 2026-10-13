package service

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestSolFirstScanRecoversOldExpiredOwnerAfterAddressReuse(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcFailoverForTest)
	order := solCursorOrder(t)
	assigned := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	deadline := assigned.Add(10 * time.Minute)
	if err := dao.Mdb.Model(order).Updates(map[string]interface{}{
		"assigned_at_ms": assigned.UnixMilli(), "payment_expires_at": deadline,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.ExpireExclusivePayments(); err != nil {
		t.Fatal(err)
	}
	// The expiry worker ran hours before the scanner's first start.
	if err := dao.Mdb.Model(order).UpdateColumn("updated_at", deadline).Error; err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	current := &mdb.Orders{TradeId: "reused-sol-owner", OrderId: "reused-sol-owner", Network: mdb.NetworkSolana, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", PaymentType: mdb.PaymentTypeEpay, Status: mdb.StatusWaitPay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(current, mdb.ChainToken{ContractAddress: USDT_Mint, Decimals: 6}, []mdb.WalletAddress{{Address: solCursorTestOwner}}, false); err != nil {
		t.Fatal(err)
	}
	l.add("timely-old-payment", 2, "3000000")
	stamp := assigned.Add(time.Minute).Unix()
	l.signatures[0].BlockTime = &stamp
	l.transactions["timely-old-payment"].(map[string]interface{})["blockTime"] = stamp
	// Transaction details are temporarily unavailable after signature discovery.
	transaction := l.transactions["timely-old-payment"]
	delete(l.transactions, "timely-old-payment")
	if err := ScanSolanaAddress(solCursorTestOwner); err == nil {
		t.Fatal("missing historical transaction was silently skipped")
	}
	assertSolCursor(t, l, "")
	l.transactions["timely-old-payment"] = transaction
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	isolateSolHistoryBackoff(t)
	clearProcessedSignatures()
	if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	assertSolCursorSettlement(t, order)
	got, err := data.GetOrderInfoByTradeId(current.TradeId)
	if err != nil || got.Status != mdb.StatusWaitPay {
		t.Fatalf("late scan changed new owner: %+v %v", got, err)
	}
	var slot mdb.PaymentAddress
	if err := dao.Mdb.Where("trade_id = ?", current.TradeId).First(&slot).Error; err != nil {
		t.Fatalf("late scan released new owner's address: %v", err)
	}
	if err := dao.Mdb.Model(order).Update("callback_confirm", mdb.CallBackConfirmOk).Error; err != nil {
		t.Fatal(err)
	}
	// Replay the same history after losing process memory, with the original
	// frozen boundary retained. Callback acknowledgement must survive.
	if err := data.SavePaymentCursor("solana:signatures:"+l.ata, ""); err != nil {
		t.Fatal(err)
	}
	clearProcessedSignatures()
	if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	got, err = data.GetOrderInfoByTradeId(order.TradeId)
	var receipts int64
	if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatalf("replay reset callback: %+v %v", got, err)
	}
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != 1 {
		t.Fatalf("receipts=%d err=%v", receipts, err)
	}
}
