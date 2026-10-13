package service

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/shopspring/decimal"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/jetton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestTonObservedMinimumUsesAssignmentSnapshot(t *testing.T) {
	for _, kind := range []string{"native", "jetton"} {
		for _, tc := range []struct {
			name             string
			original, latest float64
			amount           string
			settled, expired bool
		}{
			{"minimum-raised", 1, 4, "3", true, false},
			{"minimum-raised-after-expiry", 1, 4, "3", true, true},
			{"minimum-lowered", 4, 1, "3", false, false},
			{"below-minimum", 1, 1, "0.5", false, false},
			{"at-minimum", 1, 1, "1", true, false},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				receive := address.NewAddress(0, 0, bytes.Repeat([]byte{0x11}, 32))
				sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x22}, 32))
				wallet := address.NewAddress(0, 0, bytes.Repeat([]byte{0x33}, 32))
				token := mdb.ChainToken{BaseModel: mdb.BaseModel{ID: 1}, Network: mdb.NetworkTon, Symbol: "TON", Decimals: 9, MinAmount: tc.original, Enabled: true}
				if kind == "jetton" {
					token.Symbol, token.Decimals = "USDT", 6
					token.ContractAddress = address.NewAddress(0, 0, bytes.Repeat([]byte{0x44}, 32)).String()
				}
				expires := time.Now().Add(time.Hour)
				order := &mdb.Orders{TradeId: "ton-snapshot", OrderId: "ton-snapshot", Network: mdb.NetworkTon, Token: token.Symbol,
					Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
				if err := data.CreateExclusivePayment(order, token, []mdb.WalletAddress{{Address: receive.String()}}, false); err != nil {
					t.Fatal(err)
				}
				paidAt := time.Now().Add(-20 * time.Second).Truncate(time.Second)
				updates := map[string]interface{}{"assigned_at_ms": paidAt.Add(-time.Minute).UnixMilli()}
				if tc.expired {
					updates["payment_expires_at"] = paidAt.Add(5 * time.Second)
				}
				if err := dao.Mdb.Model(order).Updates(updates).Error; err != nil {
					t.Fatal(err)
				}
				if tc.expired {
					if err := data.ExpireExclusivePayments(); err != nil {
						t.Fatal(err)
					}
					nextExpiry := time.Now().Add(time.Hour)
					next := &mdb.Orders{TradeId: "next-owner", OrderId: "next-owner", Network: mdb.NetworkTon, Token: token.Symbol,
						Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentExpiresAt: &nextExpiry}
					if err := data.CreateExclusivePayment(next, token, []mdb.WalletAddress{{Address: receive.String()}}, false); err != nil {
						t.Fatal(err)
					}
				}
				token.MinAmount = tc.latest
				raw := decimal.RequireFromString(tc.amount).Shift(int32(token.Decimals)).BigInt()
				var native *mdb.ChainToken
				jettons := map[string]mdb.ChainToken{}
				tx := tonTestInboundTx(t, sender, receive, tlb.MustFromNano(raw, token.Decimals), cell.BeginCell().EndCell())
				if kind == "native" {
					native = &token
				} else {
					body, err := tlb.ToCell(jetton.TransferNotification{Amount: tlb.MustFromNano(raw, token.Decimals), Sender: sender, ForwardPayload: cell.BeginCell().EndCell()})
					if err != nil {
						t.Fatal(err)
					}
					tx = tonTestInboundTx(t, wallet, receive, tlb.MustFromNano(big.NewInt(1), 9), body)
					jettons[addressutil.TonRawAddressObjectKey(wallet)] = token
				}
				tx.Now = uint32(paidAt.Unix())
				transfer, err := ParseTonInboundTransfer(tx, receive, native, jettons)
				if err != nil || transfer == nil {
					t.Fatalf("received funds discarded before ownership/minimum validation: %+v %v", transfer, err)
				}
				if err := ProcessTonTransfer(transfer); err != nil {
					t.Fatal(err)
				}
				var receipt mdb.PaymentReceipt
				if err := dao.Mdb.Where("trade_id = ?", order.TradeId).First(&receipt).Error; err != nil {
					t.Fatal(err)
				}
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				if tc.settled {
					want := decimal.RequireFromString(tc.amount).Mul(decimal.NewFromInt(7)).InexactFloat64()
					if got.Status != mdb.StatusPaySuccess || got.PaidAmount != want || receipt.Status != "settled" {
						t.Fatalf("valid payment not settled: %+v receipt=%+v", got, receipt)
					}
					if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
						t.Fatal(err)
					}
				} else if got.Status != mdb.StatusWaitPay || receipt.Status != "exception" || receipt.Reason != "below_minimum" {
					t.Fatalf("minimum not enforced: %+v receipt=%+v", got, receipt)
				}
				if err := ProcessTonTransfer(transfer); err != nil {
					t.Fatal(err)
				}
				got, err = data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || (tc.settled && got.CallBackConfirm != mdb.CallBackConfirmOk) {
					t.Fatalf("replay reset callback: %+v %v", got, err)
				}
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
					t.Fatalf("duplicate receipt: %d %v", count, err)
				}
				if tc.expired {
					next, err := data.GetOrderInfoByTradeId("next-owner")
					var slots int64
					if err != nil || next.Status != mdb.StatusWaitPay {
						t.Fatalf("credited new owner: %+v %v", next, err)
					}
					if err := dao.Mdb.Model(&mdb.PaymentAddress{}).Where("trade_id = ?", next.TradeId).Count(&slots).Error; err != nil || slots != 1 {
						t.Fatalf("released new owner's reservation: %d %v", slots, err)
					}
				}
			})
		}
	}
}

func TestTonAmountLockAndManualKeepConfiguredMinimum(t *testing.T) {
	for _, symbol := range []string{"TON", "USDT"} {
		for _, amount := range []float64{0.5, 1} {
			t.Run(symbol+"/"+decimal.NewFromFloat(amount).String(), func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				receive := address.NewAddress(0, 0, bytes.Repeat([]byte{0x55}, 32)).String()
				token, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkTon, symbol)
				if err != nil {
					t.Fatal(err)
				}
				if token.ID == 0 {
					*token = mdb.ChainToken{Network: mdb.NetworkTon, Symbol: symbol, Decimals: 9, Enabled: true}
					if symbol == "USDT" {
						token.Decimals = 6
						token.ContractAddress = address.NewAddress(0, 0, bytes.Repeat([]byte{0x44}, 32)).String()
					}
					if err := dao.Mdb.Create(token).Error; err != nil {
						t.Fatal(err)
					}
				}
				if err := data.LockTransaction(mdb.NetworkTon, receive, symbol, "amount-lock", amount, time.Hour); err != nil {
					t.Fatal(err)
				}
				order := &mdb.Orders{TradeId: "amount-lock", OrderId: "amount-lock", Network: mdb.NetworkTon, Token: symbol,
					ReceiveAddress: receive, Amount: 35, ActualAmount: amount, Status: mdb.StatusWaitPay, AssignedAtMs: time.Now().Add(-time.Minute).UnixMilli()}
				if err := data.CreateOrderWithTransaction(dao.Mdb, order); err != nil {
					t.Fatal(err)
				}
				if err := dao.Mdb.Model(token).Update("min_amount", 1).Error; err != nil {
					t.Fatal(err)
				}
				transfer := &TonObservedTransfer{ReceiveAddress: receive, Token: *token, Amount: amount,
					RawAmount: decimal.NewFromFloat(amount).Shift(int32(token.Decimals)).BigInt(), BlockTimeMs: time.Now().Add(-10*time.Second).Unix() * 1000,
					BlockID: "ton-minimum-transaction", LT: 123}
				manualErr := EnsureTonTransferMatchesOrder(order, transfer)
				if (manualErr == nil) != (amount >= 1) {
					t.Fatalf("manual minimum changed: amount=%g err=%v", amount, manualErr)
				}
				if err := ProcessTonTransfer(transfer); err != nil {
					t.Fatal(err)
				}
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				var receipt mdb.PaymentReceipt
				if err := dao.Mdb.Where("trade_id = ?", order.TradeId).First(&receipt).Error; err != nil {
					t.Fatal(err)
				}
				if amount < 1 {
					if got.Status != mdb.StatusWaitPay || receipt.Reason != "below_minimum" {
						t.Fatalf("minimum bypassed: %+v receipt=%+v", got, receipt)
					}
				} else if got.Status != mdb.StatusPaySuccess || receipt.Status != "settled" {
					t.Fatalf("payment at minimum rejected: %+v receipt=%+v", got, receipt)
				}
			})
		}
	}
}
