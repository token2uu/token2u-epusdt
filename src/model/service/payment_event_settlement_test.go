package service

import (
	"errors"
	"fmt"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"gorm.io/gorm"
)

func TestFlowReviewManualAndScannerDistinctEvents(t *testing.T) {
	for _, flow := range []string{"manual-first", "scanner-first", "concurrent", "receipt-write-failure"} {
		t.Run(flow, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			hash := common.HexToHash("0x" + strings.Repeat("ab", 32))
			stamp := uint64(time.Now().Add(-10 * time.Second).Unix())
			var orders []*mdb.Orders
			var entries []*types.Log
			for i, address := range []string{"0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222"} {
				orders = append(orders, createTimedAmountOrder(t, fmt.Sprintf("event-%d", i), mdb.NetworkEthereum, address, 5, time.Minute))
				entries = append(entries, &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(address).Bytes())}, Data: common.LeftPadBytes(big.NewInt(5_000_000).Bytes(), 32), TxHash: hash, BlockNumber: 100, Index: uint(i)})
			}
			receipt := newPaymentIdentityReceipt(entries[0], stamp)
			entries[1].BlockHash = receipt.BlockHash
			receipt.Logs = entries
			server := rpc.NewServer()
			if err := server.RegisterName("eth", &paymentIdentityRPC{receipt, stamp}); err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(server)
			defer httpServer.Close()
			defer server.Stop()
			if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkEthereum, Type: mdb.RpcNodeTypeHttp, Url: httpServer.URL, Purpose: mdb.RpcNodePurposeManualVerify, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
				t.Fatal(err)
			}
			manual := func() error {
				_, err := SubmitCashierManualPayment(orders[0].TradeId, strings.ToUpper(hash.Hex()))
				return err
			}
			scan := func(i int) error {
				return ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entries[i].Address, common.HexToAddress(orders[i].ReceiveAddress), big.NewInt(5_000_000), hash.Hex(), int64(stamp)*1000, uint(i))
			}
			switch flow {
			case "manual-first":
				if err := manual(); err != nil {
					t.Fatal(err)
				}
				if err := scan(1); err != nil {
					t.Fatal(err)
				}
			case "scanner-first":
				if err := scan(1); err != nil {
					t.Fatal(err)
				}
				if err := manual(); err != nil {
					t.Fatal(err)
				}
			case "concurrent":
				done := make(chan error, 2)
				go func() { done <- manual() }()
				go func() { done <- scan(1) }()
				for i := 0; i < 2; i++ {
					if err := <-done; err != nil {
						t.Error(err)
					}
				}
			case "receipt-write-failure":
				if err := dao.Mdb.Callback().Create().Before("gorm:create").Register("review:receipt-failure", func(tx *gorm.DB) {
					if tx.Statement.Table == "payment_receipt" {
						tx.AddError(fmt.Errorf("injected receipt failure"))
					}
				}); err != nil {
					t.Fatal(err)
				}
				err := manual()
				if removeErr := dao.Mdb.Callback().Create().Remove("review:receipt-failure"); removeErr != nil {
					t.Fatal(removeErr)
				}
				if err == nil {
					t.Fatal("expected write failure")
				}
				before, err := data.GetOrderInfoByTradeId(orders[0].TradeId)
				if err != nil || before.Status != mdb.StatusWaitPay {
					t.Fatalf("settlement not rolled back: %+v %v", before, err)
				}
				if err := manual(); err != nil {
					t.Fatal(err)
				}
				if err := scan(1); err != nil {
					t.Fatal(err)
				}
			}
			for i, order := range orders {
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.Status != mdb.StatusPaySuccess {
					t.Fatalf("event %d not paid: %+v %v", i, got, err)
				}
				if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
					t.Fatal(err)
				}
				if err := scan(i); err != nil {
					t.Fatal(err)
				}
				got, err = data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
					t.Fatalf("replay requeued callback: %+v %v", got, err)
				}
			}
			// A paid manual event must fence off future orders, not only its original order.
			next := createTimedAmountOrder(t, "next-owner", mdb.NetworkEthereum, orders[0].ReceiveAddress, 5, 0)
			if _, err := SubmitCashierManualPayment(next.TradeId, hash.Hex()); !errors.Is(err, constant.OrderBlockAlreadyProcess) && !errors.Is(err, constant.ManualPaymentVerifyErr) {
				t.Fatalf("reused settled event: %v", err)
			}
			var receipts []mdb.PaymentReceipt
			if err := dao.Mdb.Find(&receipts).Error; err != nil {
				t.Fatal(err)
			}
			if len(receipts) != 2 || receipts[0].Status != "settled" || receipts[1].Status != "settled" {
				t.Fatalf("wrong durable event claims: %+v", receipts)
			}
		})
	}
}
