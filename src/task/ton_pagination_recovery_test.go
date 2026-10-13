package task

import (
	"bytes"
	"context"
	"errors"
	"strings"
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

var errTonPaginationTestLimit = errors.New("test stopped unbounded TON pagination")

type pagedTonShardAPI struct {
	fakeTonShardAPI
	pages [][]ton.TransactionShortInfo
	cycle bool
	calls int
}

func (f *pagedTonShardAPI) WaitForBlock(uint32) ton.APIClientWrapped { return f }

func (f *pagedTonShardAPI) GetBlockTransactionsV2(ctx context.Context, _ *ton.BlockIDExt, _ uint32, _ ...*ton.TransactionID3) ([]ton.TransactionShortInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	f.calls++
	if f.calls > 10 {
		return nil, false, errTonPaginationTestLimit
	}
	i := (f.calls - 1) % len(f.pages)
	return f.pages[i], f.cycle || i < len(f.pages)-1, nil
}

func TestTonPaginationRejectsCyclesWithoutAdvancingShard(t *testing.T) {
	a := ton.TransactionShortInfo{Account: bytes.Repeat([]byte{1}, 32), LT: 1}
	b := ton.TransactionShortInfo{Account: a.Account, LT: 2}
	c := ton.TransactionShortInfo{Account: bytes.Repeat([]byte{2}, 32), LT: 2}
	master := &ton.BlockIDExt{Workchain: -1, Shard: tonMasterShard, SeqNo: 10}
	shard := &ton.BlockIDExt{Workchain: 0, Shard: tonMasterShard, SeqNo: 9}
	watch := address.NewAddress(0, 0, bytes.Repeat([]byte{3}, 32))
	state := &tonWatchState{wallets: map[string]*address.Address{addressutil.TonRawAddressObjectKey(watch): watch}}
	for _, tc := range []struct {
		name  string
		pages [][]ton.TransactionShortInfo
		cycle bool
	}{
		{"same-page", [][]ton.TransactionShortInfo{{a}, {a}}, true},
		{"cyclic-pages", [][]ton.TransactionShortInfo{{a}, {b}, {c}}, true},
		{"valid-same-account-or-lt", [][]ton.TransactionShortInfo{{a}, {b}, {c}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := &pagedTonShardAPI{pages: tc.pages, cycle: tc.cycle}
			key := tonShardCursorKey(shard)
			cursor := &tonRuntimeCursor{shardSeqNo: map[string]uint32{key: 8}}
			err := processTonShardBlocksWithCursor(context.Background(), api, master, []*ton.BlockIDExt{shard}, state, newTonJettonWalletCache(), cursor)
			if tc.cycle {
				if err == nil || errors.Is(err, errTonPaginationTestLimit) || !strings.Contains(err.Error(), "cursor") {
					t.Fatalf("scanner must reject repeated cursor itself: %v", err)
				}
				if cursor.shardSeqNo[key] != 8 {
					t.Fatalf("failed shard was consumed: %d", cursor.shardSeqNo[key])
				}
			} else if err != nil || cursor.shardSeqNo[key] != 9 {
				t.Fatalf("valid multipage block rejected: cursor=%d err=%v", cursor.shardSeqNo[key], err)
			}
		})
	}
}

func TestTonPaginationRecoverySettlesRemainingPaymentOnce(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	token := mdb.ChainToken{BaseModel: mdb.BaseModel{ID: 1}, Network: mdb.NetworkTon, Symbol: "TON", Decimals: 9, Enabled: true}
	state := &tonWatchState{wallets: make(map[string]*address.Address), nativeToken: &token}
	api := &pagedTonShardAPI{fakeTonShardAPI: fakeTonShardAPI{txByLT: make(map[uint64]*tlb.Transaction)}}
	var orders []*mdb.Orders
	paidAt := time.Now().Add(-10 * time.Second).Truncate(time.Second)
	for i, id := range []string{"ton-page-first", "ton-page-later"} {
		receive := address.NewAddress(0, 0, bytes.Repeat([]byte{byte(i + 1)}, 32))
		sender := address.NewAddress(0, 0, bytes.Repeat([]byte{9}, 32))
		expires := time.Now().Add(time.Hour)
		order := &mdb.Orders{TradeId: id, OrderId: id, Network: mdb.NetworkTon, Token: "TON", Amount: 5, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
		if err := data.CreateExclusivePayment(order, token, []mdb.WalletAddress{{Address: receive.String()}}, false); err != nil {
			t.Fatal(err)
		}
		if err := dao.Mdb.Model(order).Update("assigned_at_ms", paidAt.Add(-time.Minute).UnixMilli()).Error; err != nil {
			t.Fatal(err)
		}
		tx := &tlb.Transaction{LT: uint64(i + 1), Now: uint32(paidAt.Unix()), Hash: bytes.Repeat([]byte{byte(i + 10)}, 32)}
		tx.IO.In = &tlb.Message{MsgType: tlb.MsgTypeInternal, Msg: &tlb.InternalMessage{SrcAddr: sender, DstAddr: receive, Amount: tlb.MustFromTON("5")}}
		api.txByLT[tx.LT] = tx
		api.pages = append(api.pages, []ton.TransactionShortInfo{{Account: receive.Data(), LT: tx.LT, Hash: tx.Hash}})
		state.wallets[addressutil.TonRawAddressObjectKey(receive)] = receive
		orders = append(orders, order)
	}
	master := &ton.BlockIDExt{Workchain: -1, Shard: tonMasterShard, SeqNo: 10}
	shard := &ton.BlockIDExt{Workchain: 0, Shard: tonMasterShard, SeqNo: 9}
	key := tonShardCursorKey(shard)
	cursor := &tonRuntimeCursor{shardSeqNo: map[string]uint32{key: 8}}
	scan := func() error {
		return processTonShardBlocksWithCursor(context.Background(), api, master, []*ton.BlockIDExt{shard}, state, newTonJettonWalletCache(), cursor)
	}
	validPages := api.pages
	api.pages, api.cycle = validPages[:1], true
	if err := scan(); err == nil || errors.Is(err, errTonPaginationTestLimit) {
		t.Fatalf("broken paging did not exit through scanner error: %v", err)
	}
	if cursor.shardSeqNo[key] != 8 {
		t.Fatal("incomplete block advanced cursor")
	}
	first, err := data.GetOrderInfoByTradeId(orders[0].TradeId)
	if err != nil || first.Status != mdb.StatusPaySuccess {
		t.Fatalf("first page payment missing: %+v %v", first, err)
	}
	later, err := data.GetOrderInfoByTradeId(orders[1].TradeId)
	if err != nil || later.Status != mdb.StatusWaitPay {
		t.Fatalf("unseen payment changed: %+v %v", later, err)
	}
	if err := data.ResetCallbackConfirmOk(first.TradeId); err != nil {
		t.Fatal(err)
	}
	api.pages, api.cycle, api.calls = validPages, false, 0
	if err := scan(); err != nil || cursor.shardSeqNo[key] != 9 {
		t.Fatalf("scanner did not recover: %v", err)
	}
	for i, order := range orders {
		got, err := data.GetOrderInfoByTradeId(order.TradeId)
		if err != nil || got.Status != mdb.StatusPaySuccess || (i == 0 && got.CallBackConfirm != mdb.CallBackConfirmOk) {
			t.Fatalf("recovery lost payment or reset callback: %+v %v", got, err)
		}
		var receipts int64
		if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("trade_id = ?", order.TradeId).Count(&receipts).Error; err != nil || receipts != 1 {
			t.Fatalf("payment replay duplicated receipt: count=%d err=%v", receipts, err)
		}
	}
}
