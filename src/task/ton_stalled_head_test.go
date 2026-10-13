package task

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/spf13/viper"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

type stalledTonAPI struct {
	fakeTonShardAPI
	head   uint32
	cancel context.CancelFunc
}

func (a *stalledTonAPI) WaitForBlock(uint32) ton.APIClientWrapped { return a }
func (a *stalledTonAPI) CurrentMasterchainInfo(context.Context) (*ton.BlockIDExt, error) {
	return &ton.BlockIDExt{Workchain: -1, Shard: tonMasterShard, SeqNo: a.head}, nil
}
func (a *stalledTonAPI) LookupBlock(ctx context.Context, wc int32, shard int64, seq uint32) (*ton.BlockIDExt, error) {
	if a.cancel != nil {
		a.cancel()
	}
	return a.fakeTonShardAPI.LookupBlock(ctx, wc, shard, seq)
}

func TestTonStalledHeadFailsOverAndResumesPayment(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcFailoverForTest)
	if err := data.UpdateChainFields(mdb.NetworkTon, map[string]interface{}{"scan_interval_sec": 1, "min_confirmations": 1}); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(&mdb.ChainToken{}).Where("network = ? AND symbol <> ?", mdb.NetworkTon, "TON").Update("enabled", false).Error; err != nil {
		t.Fatal(err)
	}
	receive := address.MustParseAddr("EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I")
	seedTonScannerWallet(t)
	token, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkTon, "TON")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Minute)
	order := &mdb.Orders{TradeId: "ton-stalled", OrderId: "ton-stalled", Network: mdb.NetworkTon, Token: "TON", Amount: 5, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, *token, []mdb.WalletAddress{{Address: receive.String()}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	tx := &tlb.Transaction{LT: 2, Now: uint32(time.Now().Add(-10 * time.Second).Unix()), Hash: bytes.Repeat([]byte{0x42}, 32)}
	tx.IO.In = &tlb.Message{MsgType: tlb.MsgTypeInternal, Msg: &tlb.InternalMessage{SrcAddr: address.NewAddress(0, 0, bytes.Repeat([]byte{9}, 32)), DstAddr: receive, Amount: tlb.MustFromTON("5")}}
	shard := &ton.BlockIDExt{Workchain: 0, Shard: tonMasterShard, SeqNo: 101}
	api := &stalledTonAPI{head: 100, fakeTonShardAPI: fakeTonShardAPI{
		shards: []*ton.BlockIDExt{shard}, shorts: []ton.TransactionShortInfo{{Account: receive.Data(), LT: tx.LT, Hash: tx.Hash}}, txByLT: map[uint64]*tlb.Transaction{tx.LT: tx},
	}}
	bad := mdb.RpcNode{Network: mdb.NetworkTon, Type: mdb.RpcNodeTypeLite, Url: "https://stalled.test/global.config.json", Enabled: true, Weight: 100, Purpose: mdb.RpcNodePurposeGeneral, Status: mdb.RpcNodeStatusOk}
	good := bad
	good.Url, good.Weight = "https://healthy.test/global.config.json", 1
	if err := dao.Mdb.Where("network = ?", mdb.NetworkTon).Delete(&mdb.RpcNode{}).Error; err != nil {
		t.Fatal(err)
	}
	for _, node := range []*mdb.RpcNode{&bad, &good} {
		if err := dao.Mdb.Create(node).Error; err != nil {
			t.Fatal(err)
		}
	}
	cursor := &tonRuntimeCursor{persistent: true, initialized: true, lastMasterSeqNo: 100, shardSeqNo: map[string]uint32{tonShardCursorKey(shard): 100}}
	if err := data.SavePaymentCursor("ton:master", "100"); err != nil {
		t.Fatal(err)
	}
	run := func(node mdb.RpcNode, duration time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		if api.head > 100 {
			api.cancel = cancel
		}
		return runTonScanner(ctx, api, newTonJettonWalletCache(), cursor, node)
	}
	if err := run(bad, 30*time.Millisecond); err != nil {
		t.Fatalf("normal short idle rejected: %v", err)
	}
	viper.Set("rpc_stale_head_seconds", 1)
	for attempt := 0; attempt < data.RpcFailoverThreshold; attempt++ {
		err := run(bad, 1500*time.Millisecond)
		if service.RPCErrorReason(err) != "stale_head" {
			t.Fatalf("stalled successful node did not return failover error: %v", err)
		}
		recordTonLiteNodeFailure(bad, err.Error())
	}
	saved, err := data.LoadPaymentCursor("ton:master")
	if err != nil || saved != "100" || cursor.lastMasterSeqNo != 100 || len(api.fetchedLTs) != 0 {
		t.Fatalf("stalled node consumed progress: saved=%s cursor=%d fetched=%v err=%v", saved, cursor.lastMasterSeqNo, api.fetchedLTs, err)
	}
	next, ok := resolveTonLiteNode()
	if !ok || next.ID != good.ID {
		t.Fatalf("stalled pool still selected: %+v", next)
	}
	api.head = 101
	for replay := 0; replay < 2; replay++ {
		cursor.lastMasterSeqNo, cursor.shardSeqNo[tonShardCursorKey(shard)] = 100, 100
		if err := run(next, time.Second); err != nil {
			t.Fatal(err)
		}
		got, err := data.GetOrderInfoByTradeId(order.TradeId)
		if err != nil || got.Status != mdb.StatusPaySuccess || (replay == 1 && got.CallBackConfirm != mdb.CallBackConfirmOk) {
			t.Fatalf("recovery or replay failed: %+v %v", got, err)
		}
		if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
			t.Fatal(err)
		}
	}
	saved, err = data.LoadPaymentCursor("ton:master")
	if err != nil || saved != "101" || cursor.lastMasterSeqNo != 101 {
		t.Fatalf("recovered progress not persisted: %s %+v %v", saved, cursor, err)
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("trade_id = ?", order.TradeId).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("receipt duplicated: %d %v", count, err)
	}
}
