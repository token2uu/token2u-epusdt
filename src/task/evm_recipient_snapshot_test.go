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
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type snapshotBackfillRPC struct {
	backfillIntegrityRPC
	logCalls atomic.Int32
}

func (p *snapshotBackfillRPC) GetLogs(_ context.Context, query struct{ Address []common.Address }) ([]types.Log, error) {
	p.logCalls.Add(1)
	logs := []types.Log{}
	for _, entry := range p.logs {
		for _, contract := range query.Address {
			if entry.Address == contract {
				logs = append(logs, entry)
				break
			}
		}
	}
	return logs, nil
}

func TestEvmBackfillConfigurationSnapshot(t *testing.T) {
	for _, change := range []string{"new-wallet", "new-token"} {
		for _, network := range []string{mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma} {
			t.Run(network+"/"+change, func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				data.ResetRpcEndpointPolicyForTest()
				t.Cleanup(data.ResetRpcEndpointPolicyForTest)
				if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", network).Updates(map[string]interface{}{"enabled": true, "min_confirmations": 12}).Error; err != nil {
					t.Fatal(err)
				}
				asset := mdb.ChainToken{Network: network, Symbol: "USDT", ContractAddress: "0x2222222222222222222222222222222222222222", Decimals: 6, Enabled: true}
				if err := dao.Mdb.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "network"}, {Name: "symbol"}},
					DoUpdates: clause.AssignmentColumns([]string{"contract_address", "decimals", "enabled", "min_amount"}),
				}).Create(&asset).Error; err != nil {
					t.Fatal(err)
				}
				// The listener's cache was refreshed before this wallet was enabled.
				store, watched := StoreEthRecipientsFromWallets, isWatchedEthRecipient
				switch network {
				case mdb.NetworkBsc:
					store, watched = storeBscRecipientsFromWallets, isWatchedBscRecipient
				case mdb.NetworkPolygon:
					store, watched = storePolygonRecipientsFromWallets, isWatchedPolygonRecipient
				case mdb.NetworkPlasma:
					store, watched = storePlasmaRecipientsFromWallets, isWatchedPlasmaRecipient
				}
				store(nil)
				t.Cleanup(func() { store(nil) })
				owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
				if err := dao.Mdb.Create(&mdb.WalletAddress{Network: network, Address: owner.Hex(), Status: mdb.TokenStatusEnable}).Error; err != nil {
					t.Fatal(err)
				}
				expires := time.Now().Add(time.Hour)
				order := &mdb.Orders{TradeId: "new-wallet", OrderId: "new-wallet", Network: network, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
				if err := data.CreateExclusivePayment(order, asset, []mdb.WalletAddress{{Address: owner.Hex()}}, false); err != nil {
					t.Fatal(err)
				}
				if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
					t.Fatal(err)
				}
				provider := &snapshotBackfillRPC{backfillIntegrityRPC: backfillIntegrityRPC{block: types.Header{Number: big.NewInt(101), Time: uint64(time.Now().Add(-10 * time.Second).Unix()), Difficulty: big.NewInt(1)}}}
				provider.logs = []types.Log{{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), BlockNumber: 101, BlockHash: provider.block.Hash(), TxHash: common.HexToHash("0x1234")}}
				// A node may return an extra log outside the requested recipients.
				// Removing the shared cache must not remove recipient filtering.
				unwatched := common.HexToAddress("0x3333333333333333333333333333333333333333")
				other := &mdb.Orders{TradeId: "unwatched-wallet", OrderId: "unwatched-wallet", Network: network, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
				if err := data.CreateExclusivePayment(other, asset, []mdb.WalletAddress{{Address: unwatched.Hex()}}, false); err != nil {
					t.Fatal(err)
				}
				if err := dao.Mdb.Model(other).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
					t.Fatal(err)
				}
				extra := provider.logs[0]
				extra.Topics = []common.Hash{transferEventHash, {}, common.BytesToHash(unwatched.Bytes())}
				extra.TxHash, extra.Index = common.HexToHash("0x5678"), 1
				provider.logs = append(provider.logs, extra)
				contracts := []common.Address{common.HexToAddress(asset.ContractAddress)}
				if change == "new-token" {
					// The scanner started with another asset. USDT became enabled
					// and accepted orders before the 10-second watchdog tick.
					older := mdb.ChainToken{Network: network, Symbol: "OTHER", ContractAddress: "0x4444444444444444444444444444444444444444", Decimals: 6, Enabled: true}
					if err := dao.Mdb.Create(&older).Error; err != nil {
						t.Fatal(err)
					}
					contracts = []common.Address{common.HexToAddress(older.ContractAddress)}
					store([]mdb.WalletAddress{{Address: owner.Hex()}})
				}
				server := rpc.NewServer()
				t.Cleanup(server.Stop)
				if err := server.RegisterName("eth", provider); err != nil {
					t.Fatal(err)
				}
				client := ethclient.NewClient(rpc.DialInProc(server))
				t.Cleanup(client.Close)
				run := func() {
					if err := data.UpsertEvmScanCursor(network, 100); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					const hook = "stop_after_snapshot_checkpoint"
					if err := dao.RuntimeDB.Callback().Create().After("gorm:create").Register(hook, func(tx *gorm.DB) {
						if row, ok := tx.Statement.Dest.(*mdb.EvmScanCursor); ok && row.LastBlock == 101 {
							cancel()
						}
					}); err != nil {
						t.Fatal(err)
					}
					defer dao.RuntimeDB.Callback().Create().Remove(hook)
					if err := runEvmBackfillLoop(ctx, client, network, "[TEST]", mdb.RpcNode{}, ethereum.FilterQuery{Addresses: contracts}); err != nil {
						t.Fatal(err)
					}
					cursor, err := data.GetEvmScanCursor(network)
					if err != nil || cursor.LastBlock != 101 {
						t.Fatalf("cursor=%+v err=%v", cursor, err)
					}
				}
				if change == "new-wallet" && watched(owner) {
					t.Fatal("new wallet unexpectedly already cached")
				}
				run()
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.Status != mdb.StatusPaySuccess {
					t.Fatalf("cursor advanced past new wallet's unpaid order: %+v err=%v", got, err)
				}
				if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
					t.Fatal(err)
				}
				// A subsequent cache refresh and replay cannot produce another credit.
				store([]mdb.WalletAddress{{Address: owner.Hex()}})
				run()
				got, err = data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
					t.Fatalf("replay reset callback: %+v err=%v", got, err)
				}
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
					t.Fatalf("receipts=%d err=%v", count, err)
				}
				got, err = data.GetOrderInfoByTradeId(other.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
					t.Fatalf("out-of-query recipient was credited: %+v err=%v", got, err)
				}
			})
		}
	}
}

func TestEvmBackfillUnavailableContractsRetainCursorAndRecover(t *testing.T) {
	for _, failure := range []string{"disabled", "invalid", "database-error"} {
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
			owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
			if err := dao.Mdb.Create(&mdb.WalletAddress{Network: network, Address: owner.Hex(), Status: mdb.TokenStatusEnable}).Error; err != nil {
				t.Fatal(err)
			}
			order := createEvmPaymentTestOrder(t, "configuration-recovery", network, owner.Hex(), time.Minute)
			provider := &snapshotBackfillRPC{backfillIntegrityRPC: backfillIntegrityRPC{block: types.Header{Number: big.NewInt(101), Time: uint64(time.Now().Add(-10 * time.Second).Unix()), Difficulty: big.NewInt(1)}}}
			provider.logs = []types.Log{{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), BlockNumber: 101, BlockHash: provider.block.Hash(), TxHash: common.HexToHash("0x1234")}}
			server := rpc.NewServer()
			t.Cleanup(server.Stop)
			if err := server.RegisterName("eth", provider); err != nil {
				t.Fatal(err)
			}
			client := ethclient.NewClient(rpc.DialInProc(server))
			t.Cleanup(client.Close)
			if err := data.UpsertEvmScanCursor(network, 100); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "disabled":
				if err := dao.Mdb.Model(&mdb.ChainToken{}).Where("network = ?", network).Update("enabled", false).Error; err != nil {
					t.Fatal(err)
				}
			case "invalid":
				if err := dao.Mdb.Model(asset).UpdateColumn("contract_address", "invalid").Error; err != nil {
					t.Fatal(err)
				}
			case "database-error":
				if err := dao.Mdb.Callback().Query().Before("gorm:query").Register("unavailable_token_config", func(tx *gorm.DB) {
					if tx.Statement.Table == "chain_tokens" {
						tx.AddError(fmt.Errorf("temporary token query failure"))
					}
				}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { dao.Mdb.Callback().Query().Remove("unavailable_token_config") })
			}
			query := ethereum.FilterQuery{Addresses: []common.Address{provider.logs[0].Address}}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err = runEvmBackfillLoop(ctx, client, network, "[TEST]", mdb.RpcNode{}, query)
			cancel()
			if failure != "disabled" && err == nil {
				t.Fatal("configuration failure was hidden")
			}
			cursor, err := data.GetEvmScanCursor(network)
			if err != nil || cursor.LastBlock != 100 || provider.logCalls.Load() != 0 {
				t.Fatalf("failed config sent log query or advanced cursor: %+v calls=%d err=%v", cursor, provider.logCalls.Load(), err)
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.Status != mdb.StatusWaitPay {
				t.Fatalf("invalid config changed order: %+v %v", got, err)
			}
			dao.Mdb.Callback().Query().Remove("unavailable_token_config")
			if err := dao.Mdb.Model(asset).Updates(map[string]interface{}{"enabled": true, "contract_address": provider.logs[0].Address.Hex()}).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := dao.RuntimeDB.Callback().Create().After("gorm:create").Register("stop_recovered_config", func(tx *gorm.DB) {
				if row, ok := tx.Statement.Dest.(*mdb.EvmScanCursor); ok && row.LastBlock == 101 {
					cancel()
				}
			}); err != nil {
				t.Fatal(err)
			}
			defer dao.RuntimeDB.Callback().Create().Remove("stop_recovered_config")
			if err := runEvmBackfillLoop(ctx, client, network, "[TEST]", mdb.RpcNode{}, query); err != nil {
				t.Fatal(err)
			}
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.Status != mdb.StatusPaySuccess {
				t.Fatalf("recovery lost payment: %+v %v", got, err)
			}
			cursor, err = data.GetEvmScanCursor(network)
			if err != nil || cursor.LastBlock != 101 {
				t.Fatalf("recovered cursor=%+v err=%v", cursor, err)
			}
		})
	}
}
