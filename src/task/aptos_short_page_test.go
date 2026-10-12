package task

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
)

// Model a compatible Indexer that enforces a lower per-request row limit.
type cappedAptosProvider struct {
	fakeAptosDepositProvider
	cap         int
	failOnEmpty bool
}

func (p *cappedAptosProvider) Deposits(ctx context.Context, owner string, after service.AptosEventPosition, head int64, limit int, since string) ([]service.AptosIndexedDeposit, error) {
	rows, err := p.fakeAptosDepositProvider.Deposits(ctx, owner, after, head, min(limit, p.cap), since)
	if err == nil && len(rows) == 0 && p.failOnEmpty {
		return nil, errors.New("temporary final-page failure")
	}
	return rows, err
}

func TestAptosShortPagesContinueUntilEmpty(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	order := seedAptosRegressionOrder(t, owner, state.tokens[0])
	if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
		t.Fatal(err)
	}
	p := &cappedAptosProvider{cap: 1, failOnEmpty: true, fakeAptosDepositProvider: fakeAptosDepositProvider{
		head: 110,
		rows: map[string][]service.AptosIndexedDeposit{owner: {
			indexedDeposit(owner, "0x9", 105, 0, "1"),
			indexedDeposit(owner, state.tokens[0].ContractAddress, 106, 1, "3000000"),
		}},
		transactions: map[int64][]byte{106: aptosDepositTransaction(t, owner, state.tokens[0].ContractAddress, 106)},
	}}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err == nil {
		t.Fatal("final-page failure hidden")
	}
	assertAptosRegressionOrderSettled(t, order)
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 106, Index: 1})
	if p.queryCalls != 3 || p.transactionCalls != 1 {
		t.Fatalf("short page stopped scanning: queries=%d transactions=%d", p.queryCalls, p.transactionCalls)
	}
	p.failOnEmpty = false
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 110, Index: math.MaxInt64})
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if count != 1 || p.queryCalls != 4 || p.transactionCalls != 1 {
		t.Fatalf("recovery replayed work: receipts=%d queries=%d transactions=%d", count, p.queryCalls, p.transactionCalls)
	}
}

func TestAptosShortPagesResumeAfterRoundBudget(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	order := seedAptosRegressionOrder(t, owner, state.tokens[0])
	if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
		t.Fatal(err)
	}
	rows := make([]service.AptosIndexedDeposit, 0, 11)
	for i := 0; i < aptosDepositPagesPerRound; i++ {
		rows = append(rows, indexedDeposit(owner, "0x9", 105, i, "1"))
	}
	rows = append(rows, indexedDeposit(owner, state.tokens[0].ContractAddress, 106, 1, "3000000"))
	p := &cappedAptosProvider{cap: 1, fakeAptosDepositProvider: fakeAptosDepositProvider{head: 110,
		rows:         map[string][]service.AptosIndexedDeposit{owner: rows},
		transactions: map[int64][]byte{106: aptosDepositTransaction(t, owner, state.tokens[0].ContractAddress, 106)},
	}}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 105, Index: aptosDepositPagesPerRound - 1})
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusWaitPay || p.queryCalls != aptosDepositPagesPerRound {
		t.Fatalf("round skipped backlog: order=%+v queries=%d err=%v", got, p.queryCalls, err)
	}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertAptosRegressionOrderSettled(t, order)
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 110, Index: math.MaxInt64})
	if p.queryCalls != aptosDepositPagesPerRound+2 || p.transactionCalls != 1 {
		t.Fatalf("resume did not reach payment: queries=%d transactions=%d", p.queryCalls, p.transactionCalls)
	}
}
