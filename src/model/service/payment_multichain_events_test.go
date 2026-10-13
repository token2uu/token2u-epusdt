package service

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestFlowReviewSolanaMultipleTransfers(t *testing.T) {
	ledger := setupSolCursorLedger(t)
	first := createTimedAmountOrder(t, "sol-first", mdb.NetworkSolana, solCursorTestOwner, 5, time.Minute)
	second := createTimedAmountOrder(t, "sol-second", mdb.NetworkSolana, solCursorTestOwner, 6, time.Minute)
	ledger.add("sol-multiple-events", 100, "5000000")
	transaction := ledger.transactions["sol-multiple-events"].(map[string]interface{})["transaction"].(map[string]interface{})
	message := transaction["message"].(map[string]interface{})
	instructions := message["instructions"].([]interface{})
	message["instructions"] = append(instructions, map[string]interface{}{"programId": TokenProgramID, "parsed": map[string]interface{}{"type": "transferChecked", "info": map[string]interface{}{"source": "source-ata", "destination": ledger.ata, "mint": USDT_Mint, "tokenAmount": map[string]interface{}{"amount": "6000000", "decimals": 6}}}})
	for i := 0; i < 2; i++ {
		if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
			t.Fatal(err)
		}
	}
	for _, order := range []*mdb.Orders{first, second} {
		got, err := data.GetOrderInfoByTradeId(order.TradeId)
		if err != nil || got.Status != mdb.StatusPaySuccess {
			t.Fatalf("SOL distinct event not settled: %+v %v", got, err)
		}
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("SOL event count %d %v", count, err)
	}
}

func TestFlowReviewAptosMultipleTransfers(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	asset := mdb.ChainToken{Network: mdb.NetworkAptos, Symbol: "USDT", ContractAddress: "0x1", Decimals: 6, Enabled: true}
	if err := dao.Mdb.Create(&asset).Error; err != nil {
		t.Fatal(err)
	}
	var orders []*mdb.Orders
	for i, trade := range []string{"aptos-first", "aptos-second"} {
		orders = append(orders, createTimedAmountOrder(t, trade, mdb.NetworkAptos, "0x"+strings.Repeat("0", 63)+"2", float64(5+i), time.Minute))
	}
	for replay := 0; replay < 2; replay++ {
		for i, order := range orders {
			transfer := MoveObservedTransfer{Network: mdb.NetworkAptos, ReceiveAddress: order.ReceiveAddress, Token: "USDT", AssetID: asset.ContractAddress, RawAmount: big.NewInt(int64(5+i) * 1_000_000), Decimals: 6, Amount: float64(5 + i), TxID: "aptos-batch", Version: 100, TransferKey: order.TradeId, BlockTimeMs: time.Now().Add(-5 * time.Second).UnixMilli()}
			if err := ProcessMoveObservedTransferResult(transfer); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, order := range orders {
		got, err := data.GetOrderInfoByTradeId(order.TradeId)
		if err != nil || got.Status != mdb.StatusPaySuccess {
			t.Fatalf("Aptos distinct event not settled: %+v %v", got, err)
		}
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("Aptos event count %d %v", count, err)
	}
}
