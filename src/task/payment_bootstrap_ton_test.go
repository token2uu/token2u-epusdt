package task

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type bootstrapTonAPI struct {
	*fakeTonShardAPI
	baseTime   time.Time
	failBlock  uint32
	failShards bool
}

func (a *bootstrapTonAPI) GetBlockHeader(_ context.Context, b *ton.BlockIDExt) (*tlb.BlockHeader, error) {
	if a.failBlock == b.SeqNo {
		return nil, fmt.Errorf("history unavailable")
	}
	out := &tlb.BlockHeader{}
	out.SeqNo = b.SeqNo
	out.GenUtime = uint32(a.baseTime.Add(time.Duration(b.SeqNo) * time.Second).Unix())
	return out, nil
}
func (a *bootstrapTonAPI) WaitForBlock(uint32) ton.APIClientWrapped { return a }
func (a *bootstrapTonAPI) GetBlockShardsInfo(_ context.Context, b *ton.BlockIDExt) ([]*ton.BlockIDExt, error) {
	if a.failShards {
		return nil, fmt.Errorf("shards unavailable")
	}
	return []*ton.BlockIDExt{{Workchain: 0, Shard: tonMasterShard, SeqNo: b.SeqNo}}, nil
}
func TestTonBootstrapRecoversPaymentBeforeReady(t *testing.T) {
	for _, saved := range []bool{true, false} {
		name := "without-cursor"
		if saved {
			name = "with-cursor-control"
		}
		t.Run(name, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			now := time.Now()
			receive := address.MustParseAddr("EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I")
			sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x88}, 32))
			var token mdb.ChainToken
			if err := dao.Mdb.Where("network = ? AND symbol = ?", mdb.NetworkTon, "TON").First(&token).Error; err != nil {
				t.Fatal(err)
			}
			deadline := now.Add(time.Minute)
			order := &mdb.Orders{TradeId: "ton-before-ready", OrderId: "ton-before-ready", Network: mdb.NetworkTon, Token: "TON", Amount: 21, ActualAmount: 1, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &deadline}
			if err := data.CreateExclusivePayment(order, token, []mdb.WalletAddress{{Address: receive.StringRaw()}}, false); err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(order).Update("assigned_at_ms", now.Add(-20*time.Second).UnixMilli()).Error; err != nil {
				t.Fatal(err)
			}
			tx := &tlb.Transaction{LT: 2, Now: uint32(now.Add(-10 * time.Second).Unix()), Hash: bytes.Repeat([]byte{0x99}, 32)}
			tx.IO.In = &tlb.Message{MsgType: tlb.MsgTypeInternal, Msg: &tlb.InternalMessage{SrcAddr: sender, DstAddr: receive, Amount: tlb.MustFromTON("1"), Body: cell.BeginCell().EndCell()}}
			api := &bootstrapTonAPI{fakeTonShardAPI: &fakeTonShardAPI{shortsByShardSeq: map[uint32][]ton.TransactionShortInfo{2: {{Account: receive.Data(), LT: 2, Hash: tx.Hash}}}, txByLT: map[uint64]*tlb.Transaction{2: tx}}, baseTime: now.Add(-12 * time.Second)}
			state := &tonWatchState{wallets: map[string]*address.Address{addressutil.TonRawAddressObjectKey(receive): receive}, nativeToken: &token}
			if saved {
				if err := data.SavePaymentCursor("ton:master", "1"); err != nil {
					t.Fatal(err)
				}
			}
			cursor := &tonRuntimeCursor{persistent: true}
			head := &ton.BlockIDExt{Workchain: address.MasterchainID, Shard: tonMasterShard, SeqNo: 3}
			if err := initializeTonRuntimeCursor(context.Background(), api, head, 3, cursor); err != nil {
				t.Fatal(err)
			}
			for n := cursor.lastMasterSeqNo + 1; n <= 4; n++ {
				master := &ton.BlockIDExt{Workchain: address.MasterchainID, Shard: tonMasterShard, SeqNo: n}
				if err := processTonMasterBlockWithCursor(context.Background(), api, master, state, newTonJettonWalletCache(), cursor); err != nil {
					t.Fatal(err)
				}
				cursor.lastMasterSeqNo = n
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != mdb.StatusPaySuccess {
				t.Fatalf("valid new-version TON payment skipped: status=%d cursor=%d fetched=%v", got.Status, cursor.lastMasterSeqNo, api.fetchedLTs)
			}
		})
	}
}

func TestTonBootstrapFailuresPreserveCheckpointAcrossRestart(t *testing.T) {
	for _, failure := range []string{"history", "shards"} {
		t.Run(failure, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			base := time.Now().Add(-12 * time.Second).Truncate(time.Second)
			boundary := base.Add(2 * time.Second)
			if err := data.SavePaymentCursor("ton:bootstrap_time", boundary.Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			api := &bootstrapTonAPI{fakeTonShardAPI: &fakeTonShardAPI{}, baseTime: base}
			if failure == "history" {
				api.failBlock = 2
			} else {
				api.failShards = true
			}
			head := &ton.BlockIDExt{Workchain: address.MasterchainID, Shard: tonMasterShard, SeqNo: 3}
			cursor := &tonRuntimeCursor{persistent: true}
			if err := initializeTonRuntimeCursor(context.Background(), api, head, 3, cursor); err == nil {
				t.Fatal("expected bootstrap failure")
			}
			stored, err := data.LoadPaymentCursor("ton:master")
			if err != nil || stored != "" || cursor.initialized || len(cursor.shardSeqNo) != 0 {
				t.Fatalf("failed initialization committed progress: %q %+v %v", stored, cursor, err)
			}
			api.failBlock, api.failShards = 0, false
			// Retry with a new cursor and a newer head: the frozen boundary stays at block 2.
			head.SeqNo = 4
			cursor = &tonRuntimeCursor{persistent: true}
			if err := initializeTonRuntimeCursor(context.Background(), api, head, 4, cursor); err != nil {
				t.Fatal(err)
			}
			if cursor.lastMasterSeqNo != 1 {
				t.Fatalf("retry skipped boundary block: %d", cursor.lastMasterSeqNo)
			}
			stored, err = data.LoadPaymentCursor("ton:master")
			if err != nil || stored != "1" {
				t.Fatalf("cursor=%q %v", stored, err)
			}
			// Established cursor restoration must not repeat timestamp discovery.
			api.failBlock = 4
			restarted := &tonRuntimeCursor{persistent: true}
			if err := initializeTonRuntimeCursor(context.Background(), api, head, 4, restarted); err != nil || restarted.lastMasterSeqNo != 1 {
				t.Fatalf("restart: %+v %v", restarted, err)
			}
		})
	}
}
