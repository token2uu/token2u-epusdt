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
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

type stalledEvmRPC struct {
	head     atomic.Int64
	block    types.Header
	event    types.Log
	logCalls atomic.Int32
}

func (p *stalledEvmRPC) GetBlockByNumber(_ context.Context, number string, _ bool) (*types.Header, error) {
	h := p.block
	if number == "latest" {
		h.Number = big.NewInt(p.head.Load())
	}
	return &h, nil
}

func (p *stalledEvmRPC) GetLogs(context.Context, map[string]interface{}) ([]types.Log, error) {
	p.logCalls.Add(1)
	return []types.Log{p.event}, nil
}

func TestEvmStalledHeadFailsOverWithoutLosingPayment(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcFailoverForTest)
	network := mdb.NetworkEthereum
	if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", network).Updates(map[string]interface{}{"min_confirmations": 1, "scan_interval_sec": 1}).Error; err != nil {
		t.Fatal(err)
	}
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	if _, err := data.AddWalletAddressWithNetwork(network, owner.Hex()); err != nil {
		t.Fatal(err)
	}
	order := createEvmPaymentTestOrder(t, "stalled-payment", network, owner.Hex(), time.Minute)
	asset, err := data.GetEnabledChainTokenBySymbol(network, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	p := &stalledEvmRPC{block: types.Header{Number: big.NewInt(101), Difficulty: big.NewInt(1), Time: uint64(time.Now().Add(-10 * time.Second).Unix())}}
	p.head.Store(100)
	p.event = types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(big.NewInt(5_000_000).Bytes(), 32), BlockNumber: 101, BlockHash: p.block.Hash(), TxHash: common.HexToHash("0x1234")}
	server := rpc.NewServer()
	t.Cleanup(server.Stop)
	if err := server.RegisterName("eth", p); err != nil {
		t.Fatal(err)
	}
	client := ethclient.NewClient(rpc.DialInProc(server))
	t.Cleanup(client.Close)
	bad := mdb.RpcNode{Network: network, Type: mdb.RpcNodeTypeHttp, Url: "https://stalled.test", Enabled: true, Purpose: mdb.RpcNodePurposeGeneral, Status: mdb.RpcNodeStatusOk}
	good := mdb.RpcNode{Network: network, Type: mdb.RpcNodeTypeHttp, Url: "https://recovering.test", Enabled: true, Purpose: mdb.RpcNodePurposePaidFallback, Status: mdb.RpcNodeStatusOk}
	if err := dao.Mdb.Where("network = ?", network).Delete(&mdb.RpcNode{}).Error; err != nil {
		t.Fatal(err)
	}
	for _, node := range []*mdb.RpcNode{&bad, &good} {
		if err := dao.Mdb.Create(node).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := data.UpsertEvmScanCursor(network, 100); err != nil {
		t.Fatal(err)
	}
	run := func(node mdb.RpcNode, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return runEvmBackfillLoop(ctx, client, network, "[TEST]", node, ethereum.FilterQuery{})
	}
	// A short idle period is healthy. Age is retained across reconnects so a
	// frozen endpoint cannot reset its stale timer each time the loop restarts.
	if err := run(bad, 30*time.Millisecond); err != nil {
		t.Fatalf("healthy idle head rejected: %v", err)
	}
	viper.Set("rpc_stale_head_seconds", 1)
	for attempt := 1; attempt <= data.RpcFailoverThreshold; attempt++ {
		err := run(bad, 3*time.Second)
		if service.RPCErrorReason(err) != "stale_head" {
			t.Fatalf("frozen successful RPC must trigger failover, got %v", err)
		}
		cooling := recordEvmNodeFailure("[TEST]", network, bad, err.Error())
		if cooling != (attempt == data.RpcFailoverThreshold) {
			t.Fatalf("unexpected failover threshold: attempt=%d cooling=%v", attempt, cooling)
		}
	}
	cursor, ok, err := loadEvmScanCursor(network)
	if err != nil || !ok || cursor != 100 || p.logCalls.Load() != 0 {
		t.Fatalf("stale head consumed cursor: %d %v logs=%d err=%v", cursor, ok, p.logCalls.Load(), err)
	}
	next, err := data.SelectGeneralRpcNode(network, mdb.RpcNodeTypeHttp)
	if err != nil || next == nil || next.ID != good.ID {
		t.Fatalf("stale endpoint still selected: %+v %v", next, err)
	}
	p.head.Store(101)
	for replay := 0; replay < 2; replay++ {
		if err := data.UpsertEvmScanCursor(network, 100); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		const hook = "stop_after_stalled_node_recovery"
		if err := dao.RuntimeDB.Callback().Create().After("gorm:create").Register(hook, func(tx *gorm.DB) {
			if row, ok := tx.Statement.Dest.(*mdb.EvmScanCursor); ok && row.LastBlock == 101 {
				cancel()
			}
		}); err != nil {
			cancel()
			t.Fatal(err)
		}
		err := runEvmBackfillLoop(ctx, client, network, "[TEST]", *next, ethereum.FilterQuery{})
		cancel()
		dao.RuntimeDB.Callback().Create().Remove(hook)
		if err != nil {
			t.Fatal(err)
		}
		paid, err := data.GetOrderInfoByTradeId(order.TradeId)
		if err != nil || paid.Status != mdb.StatusPaySuccess || (replay == 1 && paid.CallBackConfirm != mdb.CallBackConfirmOk) {
			t.Fatalf("payment recovery/replay failed: %+v %v", paid, err)
		}
		if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
			t.Fatal(err)
		}
	}
	var receipts int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("trade_id = ?", order.TradeId).Count(&receipts).Error; err != nil || receipts != 1 {
		t.Fatalf("duplicate payment receipts: %d %v", receipts, err)
	}
}
