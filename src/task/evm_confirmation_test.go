package task

import (
	"context"
	"fmt"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"math/big"
	"testing"
	"time"
)

func TestWebsocketMustRespectConfirmations(t *testing.T) {
	for _, mode := range []string{"removed-only-control", "unconfirmed-then-removed"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", mdb.NetworkEthereum).Update("min_confirmations", 12).Error; err != nil {
				t.Fatal(err)
			}
			owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
			order := createEvmPaymentTestOrder(t, "new-order", mdb.NetworkEthereum, owner.Hex(), time.Minute)
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			provider := &evmPaymentHeaderRPC{header: types.Header{Number: big.NewInt(101), Time: uint64(time.Now().Add(-10 * time.Second).Unix()), Difficulty: big.NewInt(1)}}
			server := rpc.NewServer()
			if err := server.RegisterName("eth", provider); err != nil {
				t.Fatal(err)
			}
			defer server.Stop()
			client := ethclient.NewClient(rpc.DialInProc(server))
			defer client.Close()
			event := types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), BlockNumber: 101, BlockHash: provider.header.Hash(), TxHash: common.HexToHash("0x1234"), Index: 0}
			watched := func(to common.Address) bool { return to == owner }
			if mode == "unconfirmed-then-removed" {
				processEvmWsTransfer(context.Background(), client, mdb.NetworkEthereum, "[TEST]", event, watched)
			}
			event.Removed = true
			processEvmWsTransfer(context.Background(), client, mdb.NetworkEthereum, "[TEST]", event, watched)
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			var receipts []mdb.PaymentReceipt
			if err := dao.Mdb.Find(&receipts).Error; err != nil {
				t.Fatal(err)
			}
			t.Logf("head=101 tx block=101 required=12 status=%d callback=%d receipts=%+v", got.Status, got.CallBackConfirm, receipts)
			if got.Status == mdb.StatusPaySuccess || len(receipts) != 0 {
				t.Fatal("unconfirmed and removed payment durably credited")
			}
		})
	}
}

type confirmationBoundaryRPC struct {
	block      types.Header
	head       int64
	failLatest bool
}

func (p *confirmationBoundaryRPC) GetBlockByNumber(_ context.Context, number string, _ bool) (*types.Header, error) {
	h := p.block
	if number == "latest" {
		if p.failLatest {
			return nil, fmt.Errorf("latest temporarily unavailable")
		}
		h.Number = big.NewInt(p.head)
	}
	return &h, nil
}

func TestWebsocketConfirmationBoundaryAndRecovery(t *testing.T) {
	for _, mode := range []string{"below-threshold", "at-threshold", "latest-failure", "canonical-hash-changed", "one-confirmation", "short-data", "extra-topic"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			required := 12
			if mode == "one-confirmation" {
				required = 1
			}
			if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", mdb.NetworkEthereum).Update("min_confirmations", required).Error; err != nil {
				t.Fatal(err)
			}
			owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
			order := createEvmPaymentTestOrder(t, "confirmed-order", mdb.NetworkEthereum, owner.Hex(), time.Minute)
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			provider := &confirmationBoundaryRPC{head: 112, block: types.Header{Number: big.NewInt(101), Time: uint64(time.Now().Add(-10 * time.Second).Unix()), Difficulty: big.NewInt(1)}}
			if mode == "below-threshold" {
				provider.head = 111
			}
			if mode == "one-confirmation" {
				provider.head = 101
			}
			provider.failLatest = mode == "latest-failure"
			event := types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{transferEventHash, {}, common.BytesToHash(owner.Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), BlockNumber: 101, BlockHash: provider.block.Hash(), TxHash: common.HexToHash("0x5678"), Index: 0}
			if mode == "short-data" {
				event.Data = big.NewInt(5000000).Bytes()
			}
			if mode == "extra-topic" {
				event.Topics = append(event.Topics, common.Hash{})
			}
			if mode == "canonical-hash-changed" {
				event.BlockHash = common.HexToHash("0x9999")
			}
			server := rpc.NewServer()
			if err := server.RegisterName("eth", provider); err != nil {
				t.Fatal(err)
			}
			defer server.Stop()
			client := ethclient.NewClient(rpc.DialInProc(server))
			defer client.Close()
			watched := func(to common.Address) bool { return to == owner }
			processEvmWsTransfer(context.Background(), client, mdb.NetworkEthereum, "[TEST]", event, watched)
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			accepted := mode == "at-threshold" || mode == "one-confirmation"
			if (got.Status == mdb.StatusPaySuccess) != accepted {
				t.Fatalf("wrong confirmation boundary: %+v", got)
			}
			if !accepted {
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
					t.Fatalf("deferred event consumed: %d %v", count, err)
				}
				provider.head, provider.failLatest = 112, false
				event.BlockHash = provider.block.Hash()
				event.Data = common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32)
				event.Topics = event.Topics[:3]
				if err := processEvmBackfillLogs(context.Background(), client, mdb.NetworkEthereum, "[TEST]", []types.Log{event}, watched); err != nil {
					t.Fatal(err)
				}
			}
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.Status != mdb.StatusPaySuccess {
				t.Fatalf("confirmed retry lost payment: %+v %v", got, err)
			}
			if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
				t.Fatal(err)
			}
			processEvmWsTransfer(context.Background(), client, mdb.NetworkEthereum, "[TEST]", event, watched)
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
				t.Fatalf("duplicate callback after replay: %+v %v", got, err)
			}
		})
	}
}
