package service

import (
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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

func TestManualPaymentUsesAssignmentAfterCheckoutRefresh(t *testing.T) {
	for _, network := range []string{mdb.NetworkEthereum, mdb.NetworkTron, "tron-trx", mdb.NetworkSolana} {
		for _, raw := range []int64{5000000, 3000000} {
			for _, beforeAssignment := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/raw_%d/before_assignment_%t", network, raw, beforeAssignment), func(t *testing.T) {
					var order *mdb.Orders
					var hash string
					var stamp int64
					var scan func() error
					switch network {
					case mdb.NetworkTron, "tron-trx":
						symbol := "USDT"
						if network == "tron-trx" {
							symbol = "TRX"
						}
						f := setupTronManualFixture(t, symbol)
						f.proof.Store(&tronManualProof{raw, f.txID})
						order, hash, stamp, scan = f.order, f.txID, f.stamp, f.scan
					case mdb.NetworkSolana:
						ledger := setupSolCursorLedger(t)
						isolateSolThrottle(t, 1000)
						isolateSolHistoryBackoff(t)
						if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", network).Update("min_confirmations", 1).Error; err != nil {
							t.Fatal(err)
						}
						order = createTimedAmountOrder(t, "refreshed-sol", network, solCursorTestOwner, 5, time.Minute)
						hash, stamp = "refreshed-payment", time.Now().Add(-10*time.Second).Unix()*1000
						ledger.add(hash, 2, fmt.Sprint(raw))
						seconds := stamp / 1000
						ledger.signatures[0].BlockTime = &seconds
						ledger.transactions[hash].(map[string]interface{})["blockTime"] = seconds
						scan = func() error { return ScanSolanaAddress(solCursorTestOwner) }
					default:
						t.Cleanup(testutil.SetupTestDatabases(t))
						owner := "0x1111111111111111111111111111111111111111"
						order = createTimedAmountOrder(t, "refreshed-evm", network, owner, 5, time.Minute)
						asset, err := data.GetEnabledChainTokenBySymbol(network, "USDT")
						if err != nil {
							t.Fatal(err)
						}
						hash, stamp = "0x"+strings.Repeat("ad", 32), time.Now().Add(-10*time.Second).Unix()*1000
						entry := &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(owner).Bytes())}, Data: common.LeftPadBytes(big.NewInt(raw).Bytes(), 32), TxHash: common.HexToHash(hash), BlockNumber: 100}
						receipt := newPaymentIdentityReceipt(entry, uint64(stamp/1000))
						server := rpc.NewServer()
						t.Cleanup(server.Stop)
						if err := server.RegisterName("eth", &paymentIdentityRPC{receipt, uint64(stamp / 1000)}); err != nil {
							t.Fatal(err)
						}
						httpServer := httptest.NewServer(server)
						t.Cleanup(httpServer.Close)
						if err := dao.Mdb.Create(&mdb.RpcNode{Network: network, Type: mdb.RpcNodeTypeHttp, Url: httpServer.URL, Purpose: mdb.RpcNodePurposeManualVerify, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
							t.Fatal(err)
						}
						scan = func() error {
							return ProcessConfirmedEvmERC20Transfer(network, entry.Address, common.HexToAddress(owner), big.NewInt(raw), hash, stamp, 0)
						}
					}
					if beforeAssignment {
						if err := dao.Mdb.Model(order).Update("assigned_at_ms", stamp+1000).Error; err != nil {
							t.Fatal(err)
						}
					}
					if err := data.RefreshOrderExpiration(order.TradeId); err != nil {
						t.Fatal(err)
					}
					fresh, err := data.GetOrderInfoByTradeId(order.TradeId)
					if err != nil || fresh.CreatedAt.TimestampMilli() <= stamp {
						t.Fatalf("checkout was not refreshed: %+v %v", fresh, err)
					}
					_, err = SubmitCashierManualPayment(order.TradeId, hash)
					if beforeAssignment {
						if err == nil {
							t.Fatal("pre-assignment payment was accepted")
						}
					} else {
						if err != nil {
							t.Fatalf("refresh rejected original owner's payment: %v", err)
						}
						if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
							t.Fatal(err)
						}
					}
					if err := scan(); err != nil {
						t.Fatal(err)
					}
					got, err := data.GetOrderInfoByTradeId(order.TradeId)
					if err != nil {
						t.Fatal(err)
					}
					if beforeAssignment {
						if got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
							t.Fatalf("pre-assignment payment changed order: %+v", got)
						}
					} else {
						paid := float64(0)
						if raw == 3000000 {
							paid = 21
						}
						if got.Status != mdb.StatusPaySuccess || got.PaidAmount != paid || got.CallBackConfirm != mdb.CallBackConfirmOk {
							t.Fatalf("incorrect settlement/replay: %+v", got)
						}
					}
				})
			}
		}
	}
}
