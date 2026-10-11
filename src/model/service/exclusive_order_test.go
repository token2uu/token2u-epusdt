package service

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/spf13/viper"
)

func TestExclusiveCheckoutAndTronActualPayment(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	viper.Set("epay_exclusive_addresses", true)
	for _, addr := range []string{"wallet1", "wallet2"} {
		if _, err := data.AddWalletAddress(addr); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	for i := 0; i < 2; i++ {
		req := newCreateTransactionRequest(fmt.Sprint("exclusive-", i), 5)
		req.PaymentType = mdb.PaymentTypeEpay
		response, err := CreateTransaction(req, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, response.TradeId)
		if response.ActualAmount != 5 {
			t.Fatalf("amount incremented: %v", response.ActualAmount)
		}
	}
	a, _ := data.GetOrderInfoByTradeId(ids[0])
	b, _ := data.GetOrderInfoByTradeId(ids[1])
	if a.ReceiveAddress == b.ReceiveAddress || a.CollectionMode != mdb.CollectionExclusive {
		t.Fatal("checkout not exclusive")
	}
	token, err := data.GetEnabledChainTokenBySymbol("tron", "USDT")
	if err != nil {
		t.Fatal(err)
	}
	if err := ProcessTronTRC20Transfer(*token, a.ReceiveAddress, big.NewInt(3_000_000), "trx", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	got, _ := data.GetOrderInfoByTradeId(a.TradeId)
	if got.PaidAmount != 3 || got.PaidCryptoAmount != "3" || got.Status != mdb.StatusPaySuccess {
		t.Fatalf("bad actual settlement: %+v", got)
	}
	view := buildCheckoutResponse(got)
	if view.PaidAmount != 3 || view.CollectionMode != mdb.CollectionExclusive {
		t.Fatalf("missing checkout metadata: %+v", view)
	}
}

func TestExclusivePlaceholderDoesNotExtendExpiration(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	viper.Set("epay_exclusive_addresses", true)
	_, _ = data.AddWalletAddress("wallet")
	req := newCreateTransactionRequest("placeholder-exclusive", 5)
	req.PaymentType = mdb.PaymentTypeEpay
	req.Network = ""
	req.Token = ""
	response, err := CreateTransaction(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := data.GetOrderInfoByTradeId(response.TradeId)
	view, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: before.TradeId, Network: "tron", Token: "USDT"})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := data.GetOrderInfoByTradeId(before.TradeId)
	if !after.IsSelected || !view.IsSelected {
		t.Fatal("placeholder selection was not persisted or returned")
	}
	if after.CreatedAt.TimestampMilli() != before.CreatedAt.TimestampMilli() || !after.PaymentExpiresAt.Equal(*before.PaymentExpiresAt) || view.ExpirationTime != before.PaymentExpiresAt.UnixMilli() {
		t.Fatal("network selection extended expiration")
	}
}

func TestExclusiveAptosIgnoresExpectedAmountButChecksAsset(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	expires := time.Now().Add(time.Minute)
	order := &mdb.Orders{TradeId: "aptos-exclusive", OrderId: "aptos-exclusive", Network: mdb.NetworkAptos, Token: "USDT", Currency: "CNY", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	asset := mdb.ChainToken{ContractAddress: "0x1", Decimals: 6}
	if err := data.CreateExclusivePayment(order, asset, []mdb.WalletAddress{{Address: "0xabc"}}, false); err != nil {
		t.Fatal(err)
	}
	transfer := MoveObservedTransfer{Network: mdb.NetworkAptos, ReceiveAddress: order.ReceiveAddress, Token: "USDT", AssetID: "0x1", RawAmount: big.NewInt(6_000_000), Decimals: 6, Amount: 6, TxID: "aptos-hash", BlockTimeMs: time.Now().UnixMilli(), TransferKey: "aptos:10:0"}
	if err := ProcessMoveObservedTransferResult(transfer); err != nil {
		t.Fatal(err)
	}
	if err := ProcessMoveObservedTransferResult(transfer); err != nil {
		t.Fatal(err)
	}
	got, _ := data.GetOrderInfoByTradeId(order.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.PaidAmount != 42 {
		t.Fatalf("Aptos amount conflict remains: %+v", got)
	}
	var n int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&n)
	if n != 1 {
		t.Fatal("duplicate Aptos receipt")
	}
}
