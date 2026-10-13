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
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"gorm.io/gorm"
)

type bootstrapEvmRPC struct {
	epoch       int64
	event       types.Log
	failHead    atomic.Bool
	failHistory atomic.Bool
	badHistory  string
	logCalls    atomic.Int32
	firstFrom   atomic.Int64
}

func (p *bootstrapEvmRPC) header(number int64) *types.Header {
	return &types.Header{Number: big.NewInt(number), Time: uint64(p.epoch + number*12), Difficulty: big.NewInt(1)}
}

func (p *bootstrapEvmRPC) GetBlockByNumber(_ context.Context, number string, _ bool) (*types.Header, error) {
	if number == "latest" {
		if p.failHead.Load() {
			return nil, fmt.Errorf("head unavailable")
		}
		return p.header(5011), nil
	}
	if p.failHistory.Load() {
		return nil, fmt.Errorf("history unavailable")
	}
	n, err := hexutil.DecodeUint64(number)
	if err != nil {
		return nil, err
	}
	header := p.header(int64(n))
	switch p.badHistory {
	case "null":
		return nil, nil
	case "wrong-number":
		header.Number.Add(header.Number, big.NewInt(1))
	case "zero-time":
		header.Time = 0
	case "overflow-time":
		header.Time = uint64(1<<63-1)/1000 + 1
	}
	return header, nil
}

func (p *bootstrapEvmRPC) GetLogs(_ context.Context, query struct{ FromBlock, ToBlock hexutil.Uint64 }) ([]types.Log, error) {
	if p.logCalls.Add(1) == 1 {
		p.firstFrom.Store(int64(query.FromBlock))
	}
	if uint64(query.FromBlock) <= p.event.BlockNumber && p.event.BlockNumber <= uint64(query.ToBlock) {
		return []types.Log{p.event}, nil
	}
	return []types.Log{}, nil
}

func TestEvmBootstrapRecoversPaymentBeforeFixedLookback(t *testing.T) {
	for _, scenario := range []string{"expired-before-first-start", "head-outage", "history-outage", "recent-order", "null", "wrong-number", "zero-time", "overflow-time"} {
		t.Run(scenario, func(t *testing.T) {
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
			p := &bootstrapEvmRPC{epoch: time.Now().Unix() - 5011*12}
			paymentBlock := int64(1000)
			if scenario == "recent-order" {
				paymentBlock = 4990
			}
			assigned := time.Unix(int64(p.header(paymentBlock-1).Time), 0)
			deadline := assigned.Add(time.Minute)
			expires := time.Now().Add(time.Hour)
			order := &mdb.Orders{TradeId: "bootstrap-payment", OrderId: "bootstrap-payment", Network: network, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
			if err := data.CreateExclusivePayment(order, *asset, []mdb.WalletAddress{{Address: owner.Hex()}}, false); err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"assigned_at_ms": assigned.UnixMilli(), "payment_expires_at": deadline}).Error; err != nil {
				t.Fatal(err)
			}
			if err := data.ExpireExclusivePayments(); err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(order).UpdateColumn("updated_at", deadline).Error; err != nil {
				t.Fatal(err)
			}
			currentExpires := time.Now().Add(time.Hour)
			current := &mdb.Orders{TradeId: "new-owner", OrderId: "new-owner", Network: network, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &currentExpires}
			if err := data.CreateExclusivePayment(current, *asset, []mdb.WalletAddress{{Address: owner.Hex()}}, false); err != nil {
				t.Fatal(err)
			}
			p.event = types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(big.NewInt(3000000).Bytes(), 32), BlockNumber: uint64(paymentBlock), BlockHash: p.header(paymentBlock).Hash(), TxHash: common.HexToHash("0x1234")}
			server := rpc.NewServer()
			t.Cleanup(server.Stop)
			if err := server.RegisterName("eth", p); err != nil {
				t.Fatal(err)
			}
			client := ethclient.NewClient(rpc.DialInProc(server))
			t.Cleanup(client.Close)
			run := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				const hook = "stop_after_bootstrap_payment_range"
				if err := dao.RuntimeDB.Callback().Create().After("gorm:create").Register(hook, func(tx *gorm.DB) {
					if row, ok := tx.Statement.Dest.(*mdb.EvmScanCursor); ok && p.logCalls.Load() > 0 && row.LastBlock >= paymentBlock {
						cancel()
					}
				}); err != nil {
					t.Fatal(err)
				}
				defer dao.RuntimeDB.Callback().Create().Remove(hook)
				return runEvmBackfillLoop(ctx, client, network, "[TEST]", mdb.RpcNode{}, ethereum.FilterQuery{Addresses: []common.Address{p.event.Address}})
			}
			var frozenBoundary string
			if scenario != "expired-before-first-start" && scenario != "recent-order" {
				p.failHead.Store(scenario == "head-outage")
				p.failHistory.Store(scenario == "history-outage")
				p.badHistory = scenario
				if err := run(); err == nil {
					t.Fatal("RPC outage accepted")
				}
				cursor, err := data.GetEvmScanCursor(network)
				if err != nil || cursor.ID != 0 {
					t.Fatalf("outage published unsafe cursor: %+v %v", cursor, err)
				}
				boundary, err := data.LoadPaymentCursor(network + ":bootstrap_time")
				if err != nil || boundary == "" {
					t.Fatalf("boundary not frozen before outage: %q %v", boundary, err)
				}
				frozenBoundary = boundary
				p.failHead.Store(false)
				p.failHistory.Store(false)
				p.badHistory = ""
			}
			if err := run(); err != nil {
				t.Fatal(err)
			}
			if frozenBoundary != "" {
				boundary, err := data.LoadPaymentCursor(network + ":bootstrap_time")
				if err != nil || boundary != frozenBoundary {
					t.Fatalf("retry changed frozen boundary: %q != %q: %v", boundary, frozenBoundary, err)
				}
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidAmount != 21 || got.PaidCryptoAmount != "3" {
				t.Fatalf("initial cursor skipped timely historical payment: %+v %v", got, err)
			}
			if scenario == "recent-order" && p.firstFrom.Load() != 5000-evmBackfillInitialLookbackBlocks+1 {
				t.Fatalf("existing lookback shortened: first=%d", p.firstFrom.Load())
			}
			if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
				t.Fatal(err)
			}
			if err := data.UpsertEvmScanCursor(network, paymentBlock-1); err != nil {
				t.Fatal(err)
			}
			p.failHistory.Store(false)
			if err := run(); err != nil {
				t.Fatal(err)
			}
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
				t.Fatalf("replay reset callback: %+v %v", got, err)
			}
			got, err = data.GetOrderInfoByTradeId(current.TradeId)
			if err != nil || got.Status != mdb.StatusWaitPay {
				t.Fatalf("recovery credited new owner: %+v %v", got, err)
			}
			var slot mdb.PaymentAddress
			if err := dao.Mdb.Where("trade_id = ?", current.TradeId).First(&slot).Error; err != nil {
				t.Fatalf("new address reservation lost: %v", err)
			}
			var count int64
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
				t.Fatalf("receipt count=%d err=%v", count, err)
			}
		})
	}
}

func TestEvmBootstrapResumesPersistedCursor(t *testing.T) {
	for _, saved := range []int64{0, 123, -1} {
		t.Run(fmt.Sprint(saved), func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			data.ResetRpcEndpointPolicyForTest()
			t.Cleanup(data.ResetRpcEndpointPolicyForTest)
			network := mdb.NetworkEthereum
			if err := dao.Mdb.Create(&mdb.WalletAddress{Network: network, Address: "0x1111111111111111111111111111111111111111", Status: mdb.TokenStatusEnable}).Error; err != nil {
				t.Fatal(err)
			}
			if err := data.UpsertEvmScanCursor(network, saved); err != nil {
				t.Fatal(err)
			}
			// A saved block (including zero) must not reinitialize or consult the
			// bootstrap timestamp/history. A corrupt block must stop before RPC.
			if err := data.SavePaymentCursor(network+":bootstrap_time", "invalid"); err != nil {
				t.Fatal(err)
			}
			p := &bootstrapEvmRPC{epoch: time.Now().Unix() - 5011*12}
			p.failHistory.Store(true)
			server := rpc.NewServer()
			t.Cleanup(server.Stop)
			if err := server.RegisterName("eth", p); err != nil {
				t.Fatal(err)
			}
			client := ethclient.NewClient(rpc.DialInProc(server))
			t.Cleanup(client.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			const hook = "stop_after_resumed_batch"
			if err := dao.RuntimeDB.Callback().Create().After("gorm:create").Register(hook, func(tx *gorm.DB) {
				if _, ok := tx.Statement.Dest.(*mdb.EvmScanCursor); ok {
					cancel()
				}
			}); err != nil {
				t.Fatal(err)
			}
			defer dao.RuntimeDB.Callback().Create().Remove(hook)
			err := runEvmBackfillLoop(ctx, client, network, "[TEST]", mdb.RpcNode{}, ethereum.FilterQuery{})
			cursor, loadErr := data.GetEvmScanCursor(network)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if saved < 0 {
				if err == nil || p.logCalls.Load() != 0 || cursor.LastBlock != saved {
					t.Fatalf("invalid saved cursor accepted: cursor=%+v calls=%d err=%v", cursor, p.logCalls.Load(), err)
				}
			} else if err != nil || p.logCalls.Load() != 1 || p.firstFrom.Load() != saved+1 || cursor.LastBlock != saved+evmBackfillBatchSize(network) {
				t.Fatalf("saved cursor not resumed: cursor=%+v first=%d calls=%d err=%v", cursor, p.firstFrom.Load(), p.logCalls.Load(), err)
			}
		})
	}
}
