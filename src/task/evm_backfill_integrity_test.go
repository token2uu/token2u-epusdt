package task

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"gorm.io/gorm"
)

type backfillIntegrityRPC struct {
	block     types.Header
	logs      []types.Log
	failure   string
	recovered atomic.Bool
}

func (p *backfillIntegrityRPC) GetBlockByNumber(_ context.Context, number string, _ bool) (*types.Header, error) {
	h := p.block
	if number == "latest" {
		h.Number = big.NewInt(112)
		return &h, nil
	}
	if !p.recovered.Load() {
		switch p.failure {
		case "wrong-header":
			h.Number = big.NewInt(102)
		case "null-header":
			return nil, nil
		case "overflow-time":
			h.Time = uint64(1<<63-1)/1000 + 1
		}
	}
	return &h, nil
}
func (p *backfillIntegrityRPC) GetLogs(context.Context, map[string]interface{}) ([]types.Log, error) {
	logs := append([]types.Log(nil), p.logs...)
	if !p.recovered.Load() {
		switch p.failure {
		case "orphan", "mixed-same-block":
			logs[len(logs)-1].BlockHash = common.HexToHash("0x9999")
		case "removed":
			logs[0].Removed = true
		case "outside-range":
			logs[0].BlockNumber = 102
		case "empty-data":
			logs[0].Data = nil
		case "short-data", "mixed-malformed":
			logs[len(logs)-1].Data = big.NewInt(5000000).Bytes()
		case "missing-topic":
			logs[0].Topics = logs[0].Topics[:2]
		case "extra-topic":
			logs[0].Topics = append(logs[0].Topics, common.Hash{})
		}
	}
	return logs, nil
}

func TestEvmBackfillInvalidBatchPreservesCursorAndRecovers(t *testing.T) {
	for _, failure := range []string{"none", "orphan", "removed", "wrong-header", "null-header", "overflow-time", "mixed-same-block", "outside-range", "empty-data", "short-data", "missing-topic", "extra-topic", "mixed-malformed"} {
		t.Run(failure, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			data.ResetRpcEndpointPolicyForTest()
			t.Cleanup(data.ResetRpcEndpointPolicyForTest)
			network := mdb.NetworkEthereum
			if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", network).Update("min_confirmations", 12).Error; err != nil {
				t.Fatal(err)
			}
			asset, err := data.GetEnabledChainTokenBySymbol(network, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			provider := &backfillIntegrityRPC{failure: failure, block: types.Header{Number: big.NewInt(101), Time: uint64(time.Now().Add(-10 * time.Second).Unix()), Difficulty: big.NewInt(1)}}
			var orders []*mdb.Orders
			count := 1
			if failure == "mixed-same-block" || failure == "mixed-malformed" {
				count = 2
			}
			for i := 0; i < count; i++ {
				owner := common.BigToAddress(big.NewInt(int64(i + 1)))
				if err := dao.Mdb.Create(&mdb.WalletAddress{Network: network, Address: owner.Hex(), Status: mdb.TokenStatusEnable}).Error; err != nil {
					t.Fatal(err)
				}
				orders = append(orders, createEvmPaymentTestOrder(t, owner.Hex(), network, owner.Hex(), time.Minute))
				provider.logs = append(provider.logs, types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), BlockNumber: 101, BlockHash: provider.block.Hash(), TxHash: common.BigToHash(big.NewInt(int64(i + 100))), Index: uint(i)})
			}
			if err := data.UpsertEvmScanCursor(network, 100); err != nil {
				t.Fatal(err)
			}
			server := rpc.NewServer()
			if err := server.RegisterName("eth", provider); err != nil {
				t.Fatal(err)
			}
			defer server.Stop()
			client := ethclient.NewClient(rpc.DialInProc(server))
			defer client.Close()
			watched := func(common.Address) bool { return true }
			run := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				// Stop only after the real scanner has persisted its completed range.
				const hook = "stop_after_backfill_checkpoint"
				if err := dao.RuntimeDB.Callback().Create().After("gorm:create").Register(hook, func(tx *gorm.DB) {
					if row, ok := tx.Statement.Dest.(*mdb.EvmScanCursor); ok && row.LastBlock == 101 {
						cancel()
					}
				}); err != nil {
					t.Fatal(err)
				}
				defer dao.RuntimeDB.Callback().Create().Remove(hook)
				return runEvmBackfillLoop(ctx, client, network, "[TEST]", mdb.RpcNode{}, ethereum.FilterQuery{Addresses: []common.Address{common.HexToAddress(asset.ContractAddress)}})
			}
			err = run()
			if failure != "none" {
				if err == nil {
					t.Fatal("bad batch did not fail")
				}
				cursor, e := data.GetEvmScanCursor(network)
				if e != nil || cursor.LastBlock != 100 {
					t.Fatalf("bad batch advanced cursor: %+v %v", cursor, e)
				}
				var receipts int64
				if e := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; e != nil {
					t.Fatal(e)
				}
				if receipts != 0 {
					t.Fatalf("partially credited bad batch: %d receipts", receipts)
				}
				for _, order := range orders {
					got, e := data.GetOrderInfoByTradeId(order.TradeId)
					if e != nil || got.Status != mdb.StatusWaitPay {
						t.Fatalf("bad batch changed order: %+v %v", got, e)
					}
				}
				provider.recovered.Store(true)
				if err = run(); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			cursor, err := data.GetEvmScanCursor(network)
			if err != nil || cursor.LastBlock != 101 {
				t.Fatalf("recovery cursor: %+v %v", cursor, err)
			}
			for _, order := range orders {
				got, e := data.GetOrderInfoByTradeId(order.TradeId)
				if e != nil || got.Status != mdb.StatusPaySuccess {
					t.Fatalf("recovery lost payment: %+v %v", got, e)
				}
				if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
					t.Fatal(err)
				}
			}
			if err := processEvmBackfillLogs(context.Background(), client, network, "[TEST]", provider.logs, watched); err != nil {
				t.Fatal(err)
			}
			var receipts int64
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != int64(count) {
				t.Fatalf("replay receipts=%d error=%v", receipts, err)
			}
			for _, order := range orders {
				got, e := data.GetOrderInfoByTradeId(order.TradeId)
				if e != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
					t.Fatalf("replay requeued callback: %+v %v", got, e)
				}
			}
		})
	}
}
