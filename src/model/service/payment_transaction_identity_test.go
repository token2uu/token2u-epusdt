package service

import (
	"context"
	"errors"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type paymentIdentityRPC struct {
	receipt *types.Receipt
	stamp   uint64
}

func (p *paymentIdentityRPC) GetTransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	return p.receipt, nil
}
func (p *paymentIdentityRPC) GetBlockByNumber(_ context.Context, n string, _ bool) (*types.Header, error) {
	num := int64(100)
	if n == "latest" {
		num = 110
	}
	return &types.Header{Number: big.NewInt(num), Difficulty: big.NewInt(1), Time: p.stamp, GasLimit: 30000000, Extra: []byte{}}, nil
}

// Build a complete mined receipt, including the identity fields returned by RPC.
func newPaymentIdentityReceipt(entry *types.Log, stamp uint64) *types.Receipt {
	header := &types.Header{Number: new(big.Int).SetUint64(entry.BlockNumber), Difficulty: big.NewInt(1), Time: stamp, GasLimit: 30000000, Extra: []byte{}}
	entry.BlockHash = header.Hash()
	return &types.Receipt{Status: 1, BlockNumber: header.Number, BlockHash: header.Hash(), TxHash: entry.TxHash, Logs: []*types.Log{entry}, EffectiveGasPrice: big.NewInt(1)}
}

func TestManualAndAutomaticPaymentShareTransactionIdentity(t *testing.T) {
	for _, flow := range []string{"manual-first", "scanner-first", "admin-after-scanner", "concurrent", "verified-before-scanner"} {
		for _, variant := range []string{"canonical", "uppercase", "no-prefix"} {
			t.Run(flow+"/"+variant, func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				owner := "0x1111111111111111111111111111111111111111"
				a := createTimedAmountOrder(t, "auto-owner", mdb.NetworkEthereum, owner, 5, time.Minute)
				b := createTimedAmountOrder(t, "manual-owner", mdb.NetworkEthereum, owner, 6, time.Minute)
				asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
				if err != nil {
					t.Fatal(err)
				}
				hash := common.HexToHash("0x" + strings.Repeat("ab", 32))
				stamp := uint64(time.Now().Add(-10 * time.Second).Unix())
				entry := &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(owner).Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), TxHash: hash, BlockNumber: 100, Index: 0}
				receipt := newPaymentIdentityReceipt(entry, stamp)
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
				supplied := hash.Hex()
				if variant == "uppercase" {
					supplied = strings.ToUpper(supplied)
				}
				if variant == "no-prefix" {
					supplied = strings.TrimPrefix(supplied, "0x")
				}
				scan := func() error {
					return ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), big.NewInt(5000000), hash.Hex(), int64(stamp)*1000, 0)
				}
				manual := func() error { _, err := SubmitCashierManualPayment(b.TradeId, supplied); return err }
				checkManual := func(err error) {
					t.Helper()
					if err != nil && !errors.Is(err, constant.OrderBlockAlreadyProcess) && !errors.Is(err, constant.ManualPaymentVerifyErr) {
						t.Fatalf("unexpected manual result: %v", err)
					}
				}
				switch flow {
				case "manual-first":
					if err := manual(); !errors.Is(err, constant.ManualPaymentVerifyErr) {
						t.Fatalf("manual fallback stole exact owner: %v", err)
					}
					if err := scan(); err != nil {
						t.Fatal(err)
					}
				case "scanner-first", "admin-after-scanner":
					if err := scan(); err != nil {
						t.Fatal(err)
					}
					var err error
					if flow == "admin-after-scanner" {
						_, err = SubmitManualPayment(b.TradeId, supplied)
					} else {
						err = manual()
					}
					if !errors.Is(err, constant.OrderBlockAlreadyProcess) {
						t.Fatalf("already settled payment accepted: %v", err)
					}
				case "concurrent":
					start := make(chan struct{})
					manualDone := make(chan error, 1)
					scanDone := make(chan error, 1)
					go func() { <-start; manualDone <- manual() }()
					go func() { <-start; scanDone <- scan() }()
					close(start)
					checkManual(<-manualDone)
					if err := <-scanDone; err != nil {
						t.Fatal(err)
					}
				case "verified-before-scanner":
					verified, err := ValidateManualOrderPaymentWithPaidAmount(b, supplied)
					if err != nil {
						t.Fatal(err)
					}
					if verified.CanonicalTxID != hash.Hex() {
						t.Fatalf("verification returned alias: %q", verified.CanonicalTxID)
					}
					if err := scan(); err != nil {
						t.Fatal(err)
					}
					err = OrderProcessing(&request.OrderProcessingRequest{TradeId: b.TradeId, Network: b.Network, Token: b.Token, ReceiveAddress: owner, Amount: b.ActualAmount, PaidAmount: verified.PaidAmount, ManualPayment: verified.Evidence, BlockTransactionId: verified.CanonicalTxID})
					if !errors.Is(err, constant.OrderBlockAlreadyProcess) {
						t.Fatalf("transaction failed to reject previously verified payment: %v", err)
					}
				}
				gotA, err := data.GetOrderInfoByTradeId(a.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				gotB, err := data.GetOrderInfoByTradeId(b.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				var winner *mdb.Orders
				paid := 0
				for _, o := range []*mdb.Orders{gotA, gotB} {
					if o.Status == mdb.StatusPaySuccess {
						paid++
						winner = o
					}
				}
				if paid != 1 || winner.TradeId != a.TradeId {
					t.Fatalf("expected one settlement: A=%+v B=%+v", gotA, gotB)
				}
				if winner.BlockTransactionId != hash.Hex() || winner.CallBackConfirm != mdb.CallBackConfirmNo {
					t.Fatalf("wrong persisted identity/callback: %+v", winner)
				}
				if err := data.ResetCallbackConfirmOk(winner.TradeId); err != nil {
					t.Fatal(err)
				}
				for _, alias := range []string{hash.Hex(), strings.ToUpper(hash.Hex()), strings.TrimPrefix(hash.Hex(), "0x")} {
					if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), big.NewInt(5000000), alias, int64(stamp)*1000, 0); err != nil {
						t.Fatal(err)
					}
				}
				winner, err = data.GetOrderInfoByTradeId(winner.TradeId)
				if err != nil || winner.CallBackConfirm != mdb.CallBackConfirmOk {
					t.Fatalf("replay requeued callback: %+v %v", winner, err)
				}
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
					t.Fatalf("equivalent hashes created duplicate receipts: %d %v", count, err)
				}
			})
		}
	}
}
