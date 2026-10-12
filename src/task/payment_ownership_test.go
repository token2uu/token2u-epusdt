package task

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
)

func TestAptosHistoricalOwnerSurvivesCurrentLockChanges(t *testing.T) {
	for _, paid := range []bool{false, true} {
		for _, competingAmount := range []float64{3, 4} {
			t.Run(fmt.Sprintf("paid=%v/competing=%g", paid, competingAmount), func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				owner := seedAptosScannerChain(t, "0xc")[0]
				asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
				if err != nil {
					t.Fatal(err)
				}
				create := func(id string, amount float64, age time.Duration) *mdb.Orders {
					if err := data.LockTransaction(mdb.NetworkAptos, owner, "USDT", id, amount, 10*time.Minute); err != nil {
						t.Fatal(err)
					}
					o := &mdb.Orders{TradeId: id, OrderId: id, Network: mdb.NetworkAptos, Token: "USDT", ReceiveAddress: owner, Amount: 21, ActualAmount: amount,
						Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeGmpay, NotifyUrl: "https://merchant.invalid/epay/notify"}
					if err := data.CreateOrderWithTransaction(dao.Mdb, o); err != nil {
						t.Fatal(err)
					}
					deadline := data.PaymentDeadline(o).Add(-age)
					if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", id).Updates(map[string]interface{}{"created_at": o.CreatedAt.StdTime().Add(-age), "expires_at": deadline}).Error; err != nil {
						t.Fatal(err)
					}
					if err := dao.Mdb.Model(o).Updates(map[string]interface{}{"created_at": o.CreatedAt.StdTime().Add(-age), "assigned_at_ms": o.AssignedAtMs - age.Milliseconds(), "payment_expires_at": deadline}).Error; err != nil {
						t.Fatal(err)
					}
					return o
				}
				original := create("original", 3, 11*time.Minute)
				age := time.Duration(0)
				if competingAmount != 3 {
					age = 3 * time.Minute
				}
				competing := create("competing", competingAmount, age)
				var transaction map[string]interface{}
				if err := json.Unmarshal(aptosDepositTransaction(t, owner, asset.ContractAddress, 605), &transaction); err != nil {
					t.Fatal(err)
				}
				transaction["timestamp"] = fmt.Sprint(time.Now().Add(-2 * time.Minute).UnixMicro())
				body, err := json.Marshal(transaction)
				if err != nil {
					t.Fatal(err)
				}
				provider := &fakeAptosDepositProvider{rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset.ContractAddress, 605, 1, "3000000")}}, transactions: map[int64][]byte{605: body}}
				ctx := context.Background()
				if paid {
					ctx = service.WithPaidPaymentReview(ctx)
				}
				scan := func() error { return scanAptosOwnerDeposits(ctx, provider, owner, []mdb.ChainToken{*asset}, 610) }
				// Receipt failure must roll back settlement and preserve the exact event
				// boundary; neither an ordinary scan nor paid review may consume it.
				if err := dao.Mdb.Exec(`CREATE TRIGGER reject_historical_receipt BEFORE INSERT ON payment_receipt BEGIN SELECT RAISE(ABORT, 'receipt unavailable'); END`).Error; err != nil {
					t.Fatal(err)
				}
				if err := scan(); err == nil {
					t.Fatal("receipt failure acknowledged as completed scan")
				}
				got, err := data.GetOrderInfoByTradeId(original.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
					t.Fatalf("partial settlement: %+v %v", got, err)
				}
				if cursor, err := aptosDepositCursorWithContext(ctx, owner); err != nil || cursor.Version != -1 {
					t.Fatalf("failed scan consumed cursor: %+v %v", cursor, err)
				}
				if err := dao.Mdb.Exec("DROP TRIGGER reject_historical_receipt").Error; err != nil {
					t.Fatal(err)
				}
				if err := scan(); err != nil {
					t.Fatal(err)
				}
				got, err = data.GetOrderInfoByTradeId(original.TradeId)
				if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || got.CallBackConfirm != mdb.CallBackConfirmNo {
					t.Fatalf("original not settled: %+v %v", got, err)
				}
				if expired, err := data.UpdateOrderIsExpirationById(original.ID, time.Now().Add(-10*time.Minute), true); err != nil || expired {
					t.Fatalf("paid order expired: %v %v", expired, err)
				}
				if err := data.ResetCallbackConfirmOk(original.TradeId); err != nil {
					t.Fatal(err)
				}
				if err := saveAptosDepositCursorWithContext(ctx, owner, service.AptosEventPosition{Version: -1, Index: math.MaxInt64}); err != nil {
					t.Fatal(err)
				}
				if err := scan(); err != nil {
					t.Fatal(err)
				}
				got, err = data.GetOrderInfoByTradeId(original.TradeId)
				if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk || got.CallbackNum != 0 {
					t.Fatalf("replay requeued callback: %+v %v", got, err)
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
					t.Fatalf("incorrect durable receipt: %+v", receipts)
				}
				if cursor, err := aptosDepositCursorWithContext(ctx, owner); err != nil || cursor.Version != 610 {
					t.Fatalf("completed scan did not advance: %+v %v", cursor, err)
				}
			})
		}
	}
}
