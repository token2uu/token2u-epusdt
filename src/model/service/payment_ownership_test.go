package service

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/ethereum/go-ethereum/common"
)

// Use the current reservation and order creation paths, then advance their
// timestamps together. The expiry worker deliberately has not run yet.
func createTimedAmountOrder(t *testing.T, id, network, owner string, amount float64, age time.Duration) *mdb.Orders {
	t.Helper()
	if err := data.LockTransaction(network, owner, "USDT", id, amount, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	o := &mdb.Orders{TradeId: id, OrderId: id, Network: network, Token: "USDT", ReceiveAddress: owner, Amount: 35, ActualAmount: amount,
		Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeGmpay, NotifyUrl: "https://merchant.invalid/epay/notify"}
	if err := data.CreateOrderWithTransaction(dao.Mdb, o); err != nil {
		t.Fatal(err)
	}
	deadline := data.PaymentDeadline(o).Add(-age)
	if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", id).Updates(map[string]interface{}{
		"created_at": o.CreatedAt.StdTime().Add(-age), "expires_at": deadline}).Error; err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(o).Updates(map[string]interface{}{"created_at": o.CreatedAt.StdTime().Add(-age),
		"assigned_at_ms": o.AssignedAtMs - age.Milliseconds(), "payment_expires_at": deadline}).Error; err != nil {
		t.Fatal(err)
	}
	return o
}

func TestEvmHistoricalOwnerSurvivesStreamAndBackfillOverlap(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	const owner = "0x1111111111111111111111111111111111111111"
	asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	original := createTimedAmountOrder(t, "original", mdb.NetworkEthereum, owner, 5, 11*time.Minute)
	competing := createTimedAmountOrder(t, "competing", mdb.NetworkEthereum, owner, 6, 3*time.Minute)
	stamp := time.Now().Add(-2 * time.Minute).UnixMilli()
	contract, address := common.HexToAddress(asset.ContractAddress), common.HexToAddress(owner)
	TryProcessEvmERC20Transfer(mdb.NetworkEthereum, contract, address, big.NewInt(5_000_000), "original-payment", stamp, 3)
	got, err := data.GetOrderInfoByTradeId(original.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("stream lost original ownership: %+v %v", got, err)
	}
	if err := data.ResetCallbackConfirmOk(original.TradeId); err != nil {
		t.Fatal(err)
	}
	if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, contract, address, big.NewInt(5_000_000), "original-payment", stamp, 3); err != nil {
		t.Fatal(err)
	}
	got, err = data.GetOrderInfoByTradeId(original.TradeId)
	if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatalf("backfill requeued callback: %+v %v", got, err)
	}
	current, err := data.GetOrderInfoByTradeId(competing.TradeId)
	if err != nil || current.Status != mdb.StatusWaitPay || current.BlockTransactionId != "" {
		t.Fatalf("wrong order credited: %+v %v", current, err)
	}
	lock, err := data.GetTradeIdByWalletAddressAndAmountAndToken(competing.Network, owner, competing.Token, competing.ActualAmount)
	if err != nil || lock != competing.TradeId {
		t.Fatalf("competing reservation released: %q %v", lock, err)
	}
	var receipts []mdb.PaymentReceipt
	if err := dao.Mdb.Find(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 1 || receipts[0].TradeID != original.TradeId || receipts[0].Status != "settled" {
		t.Fatalf("incorrect receipt: %+v", receipts)
	}
}

func TestSolanaHistoricalOwnerSurvivesCurrentLockChanges(t *testing.T) {
	for _, paid := range []bool{false, true} {
		for _, competingAmount := range []float64{5, 6} {
			for _, failReceipt := range []bool{false, true} {
				t.Run(fmt.Sprintf("paid=%v/competing=%g/receipt_failure=%v", paid, competingAmount, failReceipt), func(t *testing.T) {
					ledger := setupSolCursorLedger(t)
					isolateSolThrottle(t, 1000)
					isolateSolHistoryBackoff(t)
					data.ResetRpcEndpointPolicyForTest()
					t.Cleanup(data.ResetRpcEndpointPolicyForTest)
					ctx := context.Background()
					cursorKey := "solana:signatures:" + ledger.ata
					if paid {
						ctx = WithPaidPaymentReview(ctx)
						cursorKey = "solana:payment_review:" + ledger.ata
						if err := dao.Mdb.Model(&mdb.RpcNode{}).Where("network = ?", mdb.NetworkSolana).Update("purpose", mdb.RpcNodePurposePaidFallback).Error; err != nil {
							t.Fatal(err)
						}
					}
					original := createTimedAmountOrder(t, "original", mdb.NetworkSolana, solCursorTestOwner, 5, 11*time.Minute)
					age := time.Duration(0)
					if competingAmount != 5 {
						age = 3 * time.Minute
					} // A live EPay fallback overlaps the original payment.
					competing := createTimedAmountOrder(t, "competing", mdb.NetworkSolana, solCursorTestOwner, competingAmount, age)
					ledger.add("historical-payment", 2, "5000000")
					paidAt := time.Now().Add(-2 * time.Minute).Unix()
					ledger.signatures[0].BlockTime = &paidAt
					ledger.transactions["historical-payment"].(map[string]interface{})["blockTime"] = paidAt
					if failReceipt {
						if err := dao.Mdb.Exec(`CREATE TRIGGER reject_historical_receipt BEFORE INSERT ON payment_receipt BEGIN SELECT RAISE(ABORT, 'receipt unavailable'); END`).Error; err != nil {
							t.Fatal(err)
						}
						if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err == nil {
							t.Fatal("receipt failure acknowledged as completed scan")
						}
						got, err := data.GetOrderInfoByTradeId(original.TradeId)
						if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
							t.Fatalf("receipt failure partially committed: %+v %v", got, err)
						}
						if cursor, err := data.LoadPaymentCursor(cursorKey); err != nil || cursor != "" {
							t.Fatalf("failed settlement consumed cursor: %q %v", cursor, err)
						}
						if err := dao.Mdb.Exec("DROP TRIGGER reject_historical_receipt").Error; err != nil {
							t.Fatal(err)
						}
					}
					if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err != nil {
						t.Fatal(err)
					}
					got, err := data.GetOrderInfoByTradeId(original.TradeId)
					if err != nil || got.Status != mdb.StatusPaySuccess || got.BlockTransactionId != "historical-payment" || got.CallBackConfirm != mdb.CallBackConfirmNo {
						t.Fatalf("original payment not settled: %+v %v", got, err)
					}
					// The delayed expiry worker cannot overwrite payment, and receipt replay
					// after losing the volatile signature cache cannot enqueue another callback.
					if expired, err := data.UpdateOrderIsExpirationById(original.ID, time.Now().Add(-10*time.Minute), true); err != nil || expired {
						t.Fatalf("paid order expired: %v %v", expired, err)
					}
					if err := data.ResetCallbackConfirmOk(original.TradeId); err != nil {
						t.Fatal(err)
					}
					clearProcessedSignatures()
					if err := data.SavePaymentCursor(cursorKey, ""); err != nil {
						t.Fatal(err)
					}
					if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err != nil {
						t.Fatal(err)
					}
					got, err = data.GetOrderInfoByTradeId(original.TradeId)
					if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk || got.CallbackNum != 0 {
						t.Fatalf("replay requeued callback: %+v %v", got, err)
					}
					current, err := data.GetOrderInfoByTradeId(competing.TradeId)
					if err != nil || current.Status != mdb.StatusWaitPay || current.BlockTransactionId != "" {
						t.Fatalf("payment credited to competing order: %+v %v", current, err)
					}
					lock, err := data.GetTradeIdByWalletAddressAndAmountAndToken(competing.Network, competing.ReceiveAddress, competing.Token, competing.ActualAmount)
					if err != nil || lock != competing.TradeId {
						t.Fatalf("competing reservation released: %q %v", lock, err)
					}
					var receipts []mdb.PaymentReceipt
					if err := dao.Mdb.Where("tx_hash = ?", "historical-payment").Find(&receipts).Error; err != nil {
						t.Fatal(err)
					}
					if len(receipts) != 1 || receipts[0].TradeID != original.TradeId || receipts[0].Status != "settled" {
						t.Fatalf("incorrect durable receipt: %+v", receipts)
					}
					if cursor, err := data.LoadPaymentCursor(cursorKey); err != nil || cursor != "historical-payment" {
						t.Fatalf("successful scan did not advance: %q %v", cursor, err)
					}
				})
			}
		}
	}
}

func TestEvmMissingChainTimeCannotUseCurrentReservation(t *testing.T) {
	for _, stamp := range []int64{0, -1} {
		t.Run(fmt.Sprint(stamp), func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			const owner = "0x1111111111111111111111111111111111111111"
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			createTimedAmountOrder(t, "current", mdb.NetworkEthereum, owner, 5, time.Minute)
			contract, address := common.HexToAddress(asset.ContractAddress), common.HexToAddress(owner)
			TryProcessEvmERC20Transfer(mdb.NetworkEthereum, contract, address, big.NewInt(5_000_000), "missing-time", stamp, 3)
			if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, contract, address, big.NewInt(5_000_000), "missing-time", stamp, 3); err == nil {
				t.Fatal("missing time acknowledged by confirmed scanner")
			}
			got, err := data.GetOrderInfoByTradeId("current")
			if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
				t.Fatalf("missing time settled order: %+v %v", got, err)
			}
			var receipts int64
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != 0 {
				t.Fatalf("missing time persisted: %d %v", receipts, err)
			}
			lock, err := data.GetTradeIdByWalletAddressAndAmountAndToken(mdb.NetworkEthereum, owner, "USDT", 5)
			if err != nil || lock != "current" {
				t.Fatalf("missing time released reservation: %q %v", lock, err)
			}
		})
	}
}
