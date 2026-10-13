package service

import (
	"errors"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/spf13/viper"
)

func TestCheckoutRejectsUnpayableMinimumWithoutReserving(t *testing.T) {
	for _, mode := range []string{"exclusive", "amount-lock"} {
		for _, entry := range []string{"create", "select", "switch"} {
			t.Run(mode+"/"+entry, func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				viper.Set("epay_exclusive_addresses", mode == "exclusive")
				for _, network := range []string{mdb.NetworkTron, mdb.NetworkEthereum} {
					if _, err := data.AddWalletAddressWithNetwork(network, "0x1111111111111111111111111111111111111111"); err != nil {
						t.Fatal(err)
					}
				}
				asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkTron, "USDT")
				if err != nil || asset.ID == 0 {
					t.Fatalf("asset: %+v %v", asset, err)
				}
				// The configured minimum may have more precision than checkout
				// amounts. Rounding it down would still produce an unpaid order.
				if err := dao.Mdb.Model(asset).Update("min_amount", 5.001).Error; err != nil {
					t.Fatal(err)
				}
				req := newCreateTransactionRequest("minimum-checkout", 5)
				req.PaymentType = mdb.PaymentTypeEpay
				var parent *mdb.Orders
				if entry != "create" {
					if entry == "select" {
						req.Network, req.Token = "", ""
					} else {
						req.Network = mdb.NetworkEthereum
					}
					created, err := CreateTransaction(req, nil)
					if err != nil {
						t.Fatal(err)
					}
					parent, err = data.GetOrderInfoByTradeId(created.TradeId)
					if err != nil {
						t.Fatal(err)
					}
				}
				attempt := func() (string, error) {
					if entry == "create" {
						result, err := CreateTransaction(req, nil)
						if err != nil {
							return "", err
						}
						return result.TradeId, nil
					}
					result, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Network: mdb.NetworkTron, Token: "USDT"})
					if err != nil {
						return "", err
					}
					return result.TradeId, nil
				}
				if _, err := attempt(); !errors.Is(err, constant.PayAmountErr) {
					t.Fatalf("checkout requests 5 below minimum 5.001: error = %v, want %v", err, constant.PayAmountErr)
				}
				var orders []mdb.Orders
				if err := dao.Mdb.Order("id").Find(&orders).Error; err != nil {
					t.Fatal(err)
				}
				if parent == nil {
					if len(orders) != 0 {
						t.Fatalf("rejected checkout persisted orders: %+v", orders)
					}
				} else if len(orders) != 1 || !reflect.DeepEqual(orders[0], *parent) {
					t.Fatalf("rejected selection changed parent or created child: before=%+v after=%+v", parent, orders)
				}
				var slots, locks int64
				if err := dao.Mdb.Model(&mdb.PaymentAddress{}).Where("network = ?", mdb.NetworkTron).Count(&slots).Error; err != nil {
					t.Fatal(err)
				}
				if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("network = ?", mdb.NetworkTron).Count(&locks).Error; err != nil {
					t.Fatal(err)
				}
				if slots != 0 || locks != 0 {
					t.Fatalf("rejected checkout reserved capacity: slots=%d locks=%d", slots, locks)
				}
				// A corrected minimum must permit retry, including selection of
				// the same placeholder and creation of the parent's only child.
				if err := dao.Mdb.Model(asset).Update("min_amount", 5).Error; err != nil {
					t.Fatal(err)
				}
				tradeID, err := attempt()
				if err != nil {
					t.Fatal(err)
				}
				order, err := data.GetOrderInfoByTradeId(tradeID)
				if err != nil || order.ActualAmount != 5 || order.Status != mdb.StatusWaitPay {
					t.Fatalf("at-minimum checkout: %+v %v", order, err)
				}
				paidAt := time.Now().UnixMilli()
				for replay := 0; replay < 2; replay++ {
					if err := ProcessTronTRC20Transfer(*asset, order.ReceiveAddress, big.NewInt(5_000_000), "minimum-payment", paidAt); err != nil {
						t.Fatal(err)
					}
				}
				paid, err := data.GetOrderInfoByTradeId(tradeID)
				if err != nil || paid.Status != mdb.StatusPaySuccess {
					t.Fatalf("exact displayed payment not settled: %+v %v", paid, err)
				}
				var receipts int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("trade_id = ? AND status = ?", tradeID, "settled").Count(&receipts).Error; err != nil || receipts != 1 {
					t.Fatalf("settlement/replay receipts=%d err=%v", receipts, err)
				}
			})
		}
	}
}

func TestCheckoutMinimumPreservesExistingSelectionAndAmountIncrement(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		t.Run(map[bool]string{false: "amount-lock", true: "exclusive"}[exclusive], func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			viper.Set("epay_exclusive_addresses", exclusive)
			if _, err := data.AddWalletAddress("minimum-wallet"); err != nil {
				t.Fatal(err)
			}
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkTron, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(asset).Update("min_amount", 5).Error; err != nil {
				t.Fatal(err)
			}
			req := newCreateTransactionRequest("existing-minimum", 5)
			req.PaymentType = mdb.PaymentTypeEpay
			first, err := CreateTransaction(req, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !exclusive {
				req.OrderId = "incremented-minimum"
				second, err := CreateTransaction(req, nil)
				if err != nil || second.ActualAmount != 5.01 || second.ReceiveAddress != first.ReceiveAddress {
					t.Fatalf("amount collision behavior changed: %+v %v", second, err)
				}
			}
			if err := dao.Mdb.Model(asset).Update("min_amount", 6).Error; err != nil {
				t.Fatal(err)
			}
			selected, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: first.TradeId, Network: mdb.NetworkTron, Token: "USDT"})
			if err != nil || selected.ActualAmount != 5 || selected.ReceiveAddress != first.ReceiveAddress {
				t.Fatalf("existing assignment changed: %+v %v", selected, err)
			}
			if exclusive {
				if err := ProcessTronTRC20Transfer(*asset, first.ReceiveAddress, big.NewInt(5_000_000), "snapshot-payment", time.Now().UnixMilli()); err != nil {
					t.Fatal(err)
				}
				paid, err := data.GetOrderInfoByTradeId(first.TradeId)
				if err != nil || paid.Status != mdb.StatusPaySuccess || paid.MinimumPayment != "5" {
					t.Fatalf("assigned minimum snapshot changed: %+v %v", paid, err)
				}
			}
		})
	}
}
