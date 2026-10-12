package task

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"gorm.io/gorm/clause"
)

type evmPaymentHeaderRPC struct {
	header    types.Header
	failure   string
	recovered atomic.Bool
}

func (p *evmPaymentHeaderRPC) GetBlockByNumber(ctx context.Context, number string, full bool) (*types.Header, error) {
	h := p.header
	if !p.recovered.Load() {
		switch p.failure {
		case "rpc_error":
			return nil, fmt.Errorf("header temporarily unavailable")
		case "null":
			return nil, nil
		case "zero_time":
			h.Time = 0
		case "overflow_time":
			h.Time = uint64(1<<63-1)/1000 + 1
		case "wrong_block":
			h.Number = new(big.Int).Add(h.Number, big.NewInt(1))
		}
	}
	return &h, nil
}

func createEvmPaymentTestOrder(t *testing.T, id, network, owner string, age time.Duration) *mdb.Orders {
	t.Helper()
	if err := data.LockTransaction(network, owner, "USDT", id, 5, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	o := &mdb.Orders{TradeId: id, OrderId: id, Network: network, Token: "USDT", ReceiveAddress: owner, Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeGmpay, CallBackConfirm: mdb.CallBackConfirmOk}
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

func TestEvmWsHeaderFailureLeavesPaymentForConfirmedBackfill(t *testing.T) {
	for _, network := range []string{mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma} {
		for _, reuse := range []bool{false, true} {
			for _, failure := range []string{"rpc_error", "null", "zero_time", "overflow_time", "wrong_block", "cancelled", "removed", "none"} {
				t.Run(fmt.Sprintf("%s/reused=%v/%s", network, reuse, failure), func(t *testing.T) {
					t.Cleanup(testutil.SetupTestDatabases(t))
					owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
					asset := mdb.ChainToken{Network: network, Symbol: "USDT", ContractAddress: "0x2222222222222222222222222222222222222222", Decimals: 6, Enabled: true}
					if err := dao.Mdb.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "network"}, {Name: "symbol"}},
						DoUpdates: clause.AssignmentColumns([]string{"contract_address", "decimals", "enabled", "min_amount"}),
					}).Create(&asset).Error; err != nil {
						t.Fatal(err)
					}
					original := createEvmPaymentTestOrder(t, "original", network, owner.Hex(), 11*time.Minute)
					if expired, err := data.UpdateOrderIsExpirationById(original.ID, time.Now().Add(-10*time.Minute), true); err != nil || !expired {
						t.Fatalf("expiry: %v %v", expired, err)
					}
					if reuse {
						createEvmPaymentTestOrder(t, "next", network, owner.Hex(), 30*time.Second)
					}
					provider := &evmPaymentHeaderRPC{failure: failure, header: types.Header{Number: big.NewInt(101), Time: uint64(time.Now().Add(-2 * time.Minute).Unix()), Difficulty: big.NewInt(1)}}
					server := rpc.NewServer()
					if err := server.RegisterName("eth", provider); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(server.Stop)
					client := ethclient.NewClient(rpc.DialInProc(server))
					t.Cleanup(client.Close)
					amount := new(big.Int).Mul(big.NewInt(5), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(asset.Decimals)), nil))
					event := types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(amount.Bytes(), 32), BlockNumber: 101, BlockHash: provider.header.Hash(), TxHash: common.HexToHash("0x1234"), Index: 3}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if failure == "cancelled" {
						cancel()
					}
					if failure == "removed" {
						event.Removed = true
					}
					watched := func(to common.Address) bool { return to == owner }
					processEvmWsTransfer(ctx, client, network, "[TEST-WS]", event, watched)
					var receipts []mdb.PaymentReceipt
					if err := dao.Mdb.Find(&receipts).Error; err != nil {
						t.Fatal(err)
					}
					got, err := data.GetOrderInfoByTradeId(original.TradeId)
					if err != nil {
						t.Fatal(err)
					}
					if failure != "none" && (len(receipts) != 0 || got.Status != mdb.StatusExpired) {
						t.Fatalf("untrusted timestamp changed payment: order=%+v receipts=%+v", got, receipts)
					}
					if failure == "none" && (len(receipts) != 1 || got.Status != mdb.StatusPaySuccess) {
						t.Fatalf("valid stream stopped settling: order=%+v receipts=%+v", got, receipts)
					}
					provider.recovered.Store(true)
					event.Removed = false
					if err := processEvmBackfillLogs(context.Background(), client, network, "[TEST-BACKFILL]", []types.Log{event}, watched); err != nil {
						t.Fatal(err)
					}
					got, err = data.GetOrderInfoByTradeId(original.TradeId)
					if err != nil || got.Status != mdb.StatusPaySuccess || got.BlockTransactionId != event.TxHash.Hex() || got.CallBackConfirm != mdb.CallBackConfirmNo {
						t.Fatalf("confirmed payment not recovered: %+v %v", got, err)
					}
					if err := data.ResetCallbackConfirmOk(original.TradeId); err != nil {
						t.Fatal(err)
					}
					// Replay through both routes after callback acknowledgement.
					processEvmWsTransfer(context.Background(), client, network, "[TEST-WS]", event, watched)
					if err := processEvmBackfillLogs(context.Background(), client, network, "[TEST-BACKFILL]", []types.Log{event}, watched); err != nil {
						t.Fatal(err)
					}
					got, err = data.GetOrderInfoByTradeId(original.TradeId)
					if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
						t.Fatalf("replay requeued callback: %+v %v", got, err)
					}
					if err := dao.Mdb.Find(&receipts).Error; err != nil {
						t.Fatal(err)
					}
					if len(receipts) != 1 || receipts[0].Status != "settled" || receipts[0].TradeID != original.TradeId || receipts[0].BlockTimeMs != int64(provider.header.Time)*1000 {
						t.Fatalf("incorrect receipt: %+v", receipts)
					}
					if reuse {
						next, err := data.GetOrderInfoByTradeId("next")
						if err != nil || next.Status != mdb.StatusWaitPay || next.BlockTransactionId != "" || next.CallBackConfirm != mdb.CallBackConfirmOk {
							t.Fatalf("later order changed: %+v %v", next, err)
						}
						lock, err := data.GetTradeIdByWalletAddressAndAmountAndToken(network, owner.Hex(), "USDT", 5)
						if err != nil || lock != "next" {
							t.Fatalf("later reservation released: %q %v", lock, err)
						}
					}
				})
			}
		}
	}
}
