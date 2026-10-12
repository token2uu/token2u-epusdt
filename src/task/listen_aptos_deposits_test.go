package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
)

type fakeAptosDepositProvider struct {
	head                         int64
	rows                         map[string][]service.AptosIndexedDeposit
	transactions                 map[int64][]byte
	errors                       map[string]error
	queryCalls, transactionCalls int
	queryOverride                func(string, service.AptosEventPosition, int64, int) ([]service.AptosIndexedDeposit, error)
}

func (f *fakeAptosDepositProvider) Head(context.Context) (int64, error) { return f.head, nil }
func (f *fakeAptosDepositProvider) Deposits(_ context.Context, owner string, after service.AptosEventPosition, head int64, limit int, _ string) ([]service.AptosIndexedDeposit, error) {
	f.queryCalls++
	if f.queryOverride != nil {
		return f.queryOverride(owner, after, head, limit)
	}
	if err := f.errors[owner]; err != nil {
		return nil, err
	}
	out := []service.AptosIndexedDeposit{}
	for _, row := range f.rows[owner] {
		pos, _ := row.Position()
		if pos.After(after) && pos.Version <= head {
			out = append(out, row)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
func (f *fakeAptosDepositProvider) Transaction(version int64, validate func([]byte) error) ([]byte, error) {
	f.transactionCalls++
	if body, ok := f.transactions[version]; ok {
		if validate != nil {
			if err := validate(body); err != nil {
				return nil, err
			}
		}
		return body, nil
	}
	return nil, errors.New("RPC temporarily unavailable")
}

func indexedDeposit(owner, asset string, version int64, index int, amount string) service.AptosIndexedDeposit {
	return service.AptosIndexedDeposit{Version: json.Number(fmt.Sprint(version)), Index: json.Number(fmt.Sprint(index)), Owner: owner, Asset: asset, Type: "0x1::fungible_asset::Deposit", Amount: json.Number(amount)}
}

func aptosDepositTransaction(t *testing.T, owner, asset string, version int64) []byte {
	t.Helper()
	var rows []json.RawMessage
	if err := json.Unmarshal(aptosFungibleTransferBody(t, fmt.Sprintf("0xpayment%d", version), version, owner, asset, "3000000"), &rows); err != nil {
		t.Fatal(err)
	}
	return rows[0]
}

func assertDepositCursor(t *testing.T, owner string, want service.AptosEventPosition) {
	t.Helper()
	stored, err := data.LoadPaymentCursor("aptos:deposits:" + owner)
	var got service.AptosEventPosition
	if err != nil || json.Unmarshal([]byte(stored), &got) != nil || got != want {
		t.Fatalf("cursor=%s err=%v, want %+v", stored, err, want)
	}
}

func TestAptosDepositsMigrateRetryAndSettleOnce(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	order := &mdb.Orders{TradeId: "deposit-payment", OrderId: "deposit-payment", Network: mdb.NetworkAptos, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, *asset, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
		t.Fatal(err)
	}
	p := &fakeAptosDepositProvider{head: 1000000, rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset.ContractAddress, 105, 1, "3000000")}}}
	// A long backlog uses one address query, not thousands of full-ledger pages.
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err == nil {
		t.Fatal("missing RPC transaction accepted")
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 100, Index: math.MaxInt64})
	got, _ := data.GetOrderInfoByTradeId(order.TradeId)
	if got.Status != mdb.StatusWaitPay {
		t.Fatal("failed verification settled the order")
	}
	p.transactions = map[int64][]byte{105: aptosDepositTransaction(t, owner, asset.ContractAddress, 105)}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 1000000, Index: math.MaxInt64})
	got, _ = data.GetOrderInfoByTradeId(order.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || got.PaidAmount != 21 {
		t.Fatalf("incorrect settlement: %+v", got)
	}
	// Replay after a crash between settlement and cursor commit stays idempotent.
	if err := saveAptosDepositCursor(owner, service.AptosEventPosition{Version: 100, Index: math.MaxInt64}); err != nil {
		t.Fatal(err)
	}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("receipts=%d err=%v", count, err)
	}
	if p.queryCalls != 5 || p.transactionCalls != 3 {
		t.Fatalf("unexpected work: queries=%d RPC=%d", p.queryCalls, p.transactionCalls)
	}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	if p.queryCalls != 5 {
		t.Fatal("unchanged indexer head was re-scanned")
	}
}

func TestAptosDepositsPaginateWithinTransactionAndRespectConfirmations(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
		t.Fatal(err)
	}
	p := &fakeAptosDepositProvider{head: 106, rows: map[string][]service.AptosIndexedDeposit{}}
	for i := 0; i < 201; i++ {
		p.rows[owner] = append(p.rows[owner], indexedDeposit(owner, "0x9", 105, i, "1"))
	}
	p.rows[owner] = append(p.rows[owner], indexedDeposit(owner, "0x9", 106, 0, "1"))
	if err := scanAptosDepositRound(context.Background(), p, state, 2); err != nil {
		t.Fatal(err)
	}
	if p.queryCalls != 4 || p.transactionCalls != 0 {
		t.Fatalf("queries=%d RPC=%d", p.queryCalls, p.transactionCalls)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 105, Index: math.MaxInt64})
	p.head = 105 // Lag must never rewind a durable cursor.
	if err := scanAptosDepositRound(context.Background(), p, state, 2); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 105, Index: math.MaxInt64})
	if p.queryCalls != 4 {
		t.Fatal("indexer lag retriggered scans")
	}
}

func TestAptosDepositsFailureDoesNotBlockOtherOwners(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owners := seedAptosScannerChain(t, "0xa", "0xb")
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
		t.Fatal(err)
	}
	p := &fakeAptosDepositProvider{head: 200, errors: map[string]error{owners[0]: errors.New("429")}}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err == nil {
		t.Fatal("query failure hidden")
	}
	assertDepositCursor(t, owners[0], service.AptosEventPosition{Version: 100, Index: math.MaxInt64})
	assertDepositCursor(t, owners[1], service.AptosEventPosition{Version: 200, Index: math.MaxInt64})
	delete(p.errors, owners[0])
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owners[0], service.AptosEventPosition{Version: 200, Index: math.MaxInt64})
}

func TestAptosDepositsRejectIncompleteOrInvalidPages(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	valid := indexedDeposit(owner, state.tokens[0].ContractAddress, 105, 1, "3000000")
	badOwner, badVersion, badAmount, badAsset := valid, valid, valid, valid
	badOwner.Owner = "0xb"
	badVersion.Version = "200"
	badAmount.Amount = "-1"
	badAsset.Asset = "not-an-asset"
	for _, tc := range []struct {
		name string
		rows []service.AptosIndexedDeposit
	}{
		{"missing", nil}, {"duplicate", []service.AptosIndexedDeposit{valid, valid}},
		{"owner", []service.AptosIndexedDeposit{badOwner}}, {"future", []service.AptosIndexedDeposit{badVersion}},
		{"negative", []service.AptosIndexedDeposit{badAmount}}, {"asset", []service.AptosIndexedDeposit{badAsset}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := saveAptosDepositCursor(owner, service.AptosEventPosition{Version: 100, Index: math.MaxInt64}); err != nil {
				t.Fatal(err)
			}
			p := &fakeAptosDepositProvider{queryOverride: func(string, service.AptosEventPosition, int64, int) ([]service.AptosIndexedDeposit, error) {
				return tc.rows, nil
			}}
			if err := scanAptosOwnerDeposits(context.Background(), p, owner, state.tokens, 110); err == nil {
				t.Fatal("invalid page accepted")
			}
			assertDepositCursor(t, owner, service.AptosEventPosition{Version: 100, Index: math.MaxInt64})
			if p.transactionCalls != 0 {
				t.Fatal("page processed before complete validation")
			}
		})
	}
}

func TestAptosDepositsFreezeBootstrapAndRejectCorruptCursor(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xa")[0]
	p := &fakeAptosDepositProvider{errors: map[string]error{owner: errors.New("offline")}}
	if err := scanAptosOwnerDeposits(context.Background(), p, owner, nil, 1000); err == nil {
		t.Fatal("offline query accepted")
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: -1, Index: math.MaxInt64})
	if err := scanAptosOwnerDeposits(context.Background(), p, owner, nil, 2000); err == nil {
		t.Fatal("offline query accepted")
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: -1, Index: math.MaxInt64})
	if err := data.SavePaymentCursor("aptos:deposits:"+owner, "{}"); err != nil {
		t.Fatal(err)
	}
	if _, err := aptosDepositCursor(owner); err == nil {
		t.Fatal("corrupt cursor was reset")
	}
}

func TestAptosDepositsRejectMismatchingRPCWithoutAdvancing(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	asset := state.tokens[0].ContractAddress
	valid := aptosDepositTransaction(t, owner, asset, 105)
	for _, tc := range []struct {
		name  string
		body  []byte
		index int
	}{
		{"truncated", []byte(strings.TrimSuffix(strings.TrimSpace(string(valid)), "}")), 1},
		{"wrong_version", []byte(strings.Replace(string(valid), `"version":"105"`, `"version":"106"`, 1)), 1},
		{"failed_transaction", []byte(strings.Replace(string(valid), `"success":true`, `"success":false`, 1)), 1},
		{"amount_mismatch", []byte(strings.ReplaceAll(string(valid), `"3000000"`, `"4000000"`)), 1},
		{"wrong_event", valid, 0},
		{"wrong_owner", []byte(strings.ReplaceAll(string(valid), owner, "0xdead")), 1},
		{"wrong_asset", []byte(strings.ReplaceAll(string(valid), asset, "0xdead")), 1},
		{"missing_store_context", []byte(strings.Replace(string(valid), `"changes":[`, `"unused_changes":[`, 1)), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := service.AptosEventPosition{Version: 100, Index: math.MaxInt64}
			if err := saveAptosDepositCursor(owner, before); err != nil {
				t.Fatal(err)
			}
			p := &fakeAptosDepositProvider{rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset, 105, tc.index, "3000000")}}, transactions: map[int64][]byte{105: tc.body}}
			if err := scanAptosOwnerDeposits(context.Background(), p, owner, state.tokens, 110); err == nil {
				t.Fatal("invalid RPC transaction accepted")
			}
			assertDepositCursor(t, owner, before)
			var count int64
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("invalid RPC wrote receipts: %d %v", count, err)
			}
		})
	}
}
