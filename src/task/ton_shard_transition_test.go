package task

import (
	"bytes"
	"context"
	"fmt"
	"math/bits"
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
)

func tonTestBlockKey(b *ton.BlockIDExt) string {
	return fmt.Sprintf("%s:%d", tonShardCursorKey(b), b.SeqNo)
}

func tonTestHeader(b *ton.BlockIDExt, parents ...*ton.BlockIDExt) *tlb.BlockHeader {
	h := &tlb.BlockHeader{}
	h.SeqNo = b.SeqNo
	shift := bits.TrailingZeros64(uint64(b.Shard))
	h.Shard = tlb.ShardIdent{WorkchainID: b.Workchain, PrefixBits: int8(63 - shift), ShardPrefix: uint64(b.Shard) &^ (uint64(1) << shift)}
	ref := func(p *ton.BlockIDExt) tlb.ExtBlkRef {
		return tlb.ExtBlkRef{SeqNo: p.SeqNo, RootHash: p.RootHash, FileHash: p.FileHash}
	}
	if len(parents) > 0 {
		h.PrevRef.Prev1 = ref(parents[0])
		h.AfterSplit = len(parents) == 1 && parents[0].Shard != b.Shard
	}
	if len(parents) == 2 {
		h.AfterMerge = true
		p := ref(parents[1])
		h.PrevRef.Prev2 = &p
	}
	return h
}

type transitionTonAPI struct {
	fakeTonShardAPI
	headers      map[string]*tlb.BlockHeader
	paymentBlock string
	failHeader   bool
	failTx       bool
	failBranch   bool
}

func (a *transitionTonAPI) WaitForBlock(uint32) ton.APIClientWrapped { return a }
func (a *transitionTonAPI) GetBlockHeader(_ context.Context, b *ton.BlockIDExt) (*tlb.BlockHeader, error) {
	// Emulate a valid lightweight proof with pruned parent references. It
	// cannot be used for ancestry; the complete block below retains them.
	return tonTestHeader(b), nil
}
func (a *transitionTonAPI) GetBlockData(_ context.Context, b *ton.BlockIDExt) (*tlb.Block, error) {
	if a.failHeader {
		return nil, fmt.Errorf("header temporarily unavailable")
	}
	h := a.headers[tonTestBlockKey(b)]
	if h == nil {
		return nil, fmt.Errorf("nonexistent shard block %s", tonTestBlockKey(b))
	}
	return &tlb.Block{BlockInfo: *h}, nil
}
func (a *transitionTonAPI) LookupBlock(_ context.Context, wc int32, shard int64, seq uint32) (*ton.BlockIDExt, error) {
	b := &ton.BlockIDExt{Workchain: wc, Shard: shard, SeqNo: seq}
	if a.headers[tonTestBlockKey(b)] == nil {
		return nil, fmt.Errorf("nonexistent shard block %s", tonTestBlockKey(b))
	}
	return b, nil
}
func (a *transitionTonAPI) GetBlockTransactionsV2(_ context.Context, b *ton.BlockIDExt, _ uint32, _ ...*ton.TransactionID3) ([]ton.TransactionShortInfo, bool, error) {
	if a.failBranch && b.Shard == -0x4000000000000000 {
		return nil, false, fmt.Errorf("second merge branch temporarily unavailable")
	}
	if tonTestBlockKey(b) == a.paymentBlock {
		return a.shorts, false, nil
	}
	return nil, false, nil
}
func (a *transitionTonAPI) GetTransaction(ctx context.Context, b *ton.BlockIDExt, addr *address.Address, lt uint64) (*tlb.Transaction, error) {
	if a.failTx {
		return nil, fmt.Errorf("transaction temporarily unavailable")
	}
	return a.fakeTonShardAPI.GetTransaction(ctx, b, addr, lt)
}

func TestTonShardTransitionsRecoverEveryPayment(t *testing.T) {
	for _, topology := range []string{"split", "merge"} {
		for _, failure := range []string{"none", "header", "transaction", "second-branch"} {
			if topology == "split" && failure == "second-branch" {
				continue
			}
			t.Run(topology+"/"+failure, func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				receive := address.NewAddress(0, 0, bytes.Repeat([]byte{0x11}, 32))
				token := mdb.ChainToken{BaseModel: mdb.BaseModel{ID: 1}, Network: mdb.NetworkTon, Symbol: "TON", Decimals: 9, Enabled: true}
				expires := time.Now().Add(time.Hour)
				order := &mdb.Orders{TradeId: "ton-transition", OrderId: "ton-transition", Network: mdb.NetworkTon, Token: "TON", Amount: 5, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
				if err := data.CreateExclusivePayment(order, token, []mdb.WalletAddress{{Address: receive.String()}}, false); err != nil {
					t.Fatal(err)
				}
				if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
					t.Fatal(err)
				}
				tx := &tlb.Transaction{LT: 2, Now: uint32(time.Now().Add(-10 * time.Second).Unix()), Hash: bytes.Repeat([]byte{0x42}, 32)}
				tx.IO.In = &tlb.Message{MsgType: tlb.MsgTypeInternal, Msg: &tlb.InternalMessage{SrcAddr: address.NewAddress(0, 0, bytes.Repeat([]byte{0x22}, 32)), DstAddr: receive, Amount: tlb.MustFromTON("5")}}
				api := &transitionTonAPI{headers: make(map[string]*tlb.BlockHeader), fakeTonShardAPI: fakeTonShardAPI{shorts: []ton.TransactionShortInfo{{Account: receive.Data(), LT: tx.LT, Hash: tx.Hash}}, txByLT: map[uint64]*tlb.Transaction{tx.LT: tx}}}
				block := func(shard int64, seq uint32) *ton.BlockIDExt {
					return &ton.BlockIDExt{Workchain: 0, Shard: shard, SeqNo: seq}
				}
				left, right := int64(0x4000000000000000), int64(-0x4000000000000000)
				baseline := map[string]uint32{}
				link := func(b *ton.BlockIDExt, parents ...*ton.BlockIDExt) {
					api.headers[tonTestBlockKey(b)] = tonTestHeader(b, parents...)
				}
				var tip, payment *ton.BlockIDExt
				if topology == "split" {
					old := block(tonMasterShard, 10)
					baseline[tonShardCursorKey(old)] = 10
					payment, tip = block(left, 11), block(left, 12)
					link(payment, old)
					link(tip, payment)
				} else {
					baseline[tonShardCursorKey(block(tonMasterShard, 8))] = 8
					baseline[tonShardCursorKey(block(left, 10))] = 10
					baseline[tonShardCursorKey(block(right, 10))] = 10
					payment, tip = block(left, 11), block(tonMasterShard, 13)
					merged := block(tonMasterShard, 12)
					link(payment, block(left, 10))
					link(merged, payment, block(right, 11))
					link(tip, merged)
				}
				api.paymentBlock = tonTestBlockKey(payment)
				state := &tonWatchState{wallets: map[string]*address.Address{addressutil.TonRawAddressObjectKey(receive): receive}, nativeToken: &token}
				newCursor := func() *tonRuntimeCursor {
					c := &tonRuntimeCursor{initialized: true, lastMasterSeqNo: 100, shardSeqNo: make(map[string]uint32)}
					for k, v := range baseline {
						c.shardSeqNo[k] = v
					}
					return c
				}
				cursor := newCursor()
				scan := func() error {
					return processTonShardBlocksWithCursor(context.Background(), api, &ton.BlockIDExt{Workchain: -1, Shard: tonMasterShard, SeqNo: 101}, []*ton.BlockIDExt{tip}, state, newTonJettonWalletCache(), cursor)
				}
				if failure != "none" {
					api.failHeader, api.failTx = failure == "header", failure == "transaction"
					api.failBranch = failure == "second-branch"
					if err := scan(); err == nil {
						t.Fatal("failed history read advanced the scan")
					}
					if cursor.shardSeqNo[tonShardCursorKey(tip)] >= tip.SeqNo {
						t.Fatal("failed shard marked complete")
					}
					api.failHeader, api.failTx = false, false
					api.failBranch = false
				}
				for replay := 0; replay < 2; replay++ {
					if replay == 1 {
						cursor = newCursor()
					} // Replay from the last completed master after restart.
					if err := scan(); err != nil {
						t.Fatal(err)
					}
					got, err := data.GetOrderInfoByTradeId(order.TradeId)
					if err != nil || got.Status != mdb.StatusPaySuccess || (replay == 1 && got.CallBackConfirm != mdb.CallBackConfirmOk) {
						t.Fatalf("transition lost payment or reset callback: %+v %v", got, err)
					}
					if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
						t.Fatal(err)
					}
				}
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("trade_id = ?", order.TradeId).Count(&count).Error; err != nil || count != 1 {
					t.Fatalf("duplicate receipts: %d %v", count, err)
				}
			})
		}
	}
}

func TestTonEmptyShardCheckpointRetainsInitialTips(t *testing.T) {
	// No preexisting shard boundary: processing one initial tip must not turn
	// the other initial tip into an unbounded history walk.
	receive := address.NewAddress(0, 0, bytes.Repeat([]byte{0x11}, 32))
	state := &tonWatchState{wallets: map[string]*address.Address{addressutil.TonRawAddressObjectKey(receive): receive}}
	shards := []*ton.BlockIDExt{{Workchain: 0, Shard: 0x4000000000000000, SeqNo: 100}, {Workchain: 0, Shard: -0x4000000000000000, SeqNo: 110}}
	api := &transitionTonAPI{failHeader: true}
	cursor := &tonRuntimeCursor{}
	if err := processTonShardBlocksWithCursor(context.Background(), api, &ton.BlockIDExt{SeqNo: 200}, shards, state, newTonJettonWalletCache(), cursor); err != nil {
		t.Fatal(err)
	}
	for _, shard := range shards {
		if cursor.shardSeqNo[tonShardCursorKey(shard)] != shard.SeqNo {
			t.Fatal("initial tip was not processed")
		}
	}
}
