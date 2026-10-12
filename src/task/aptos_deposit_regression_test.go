package task

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/spf13/viper"
)

func seedAptosRegressionOrder(t *testing.T, owner string, token mdb.ChainToken) *mdb.Orders {
	t.Helper()
	expires := time.Now().Add(time.Hour)
	order := &mdb.Orders{TradeId: t.Name(), OrderId: t.Name(), Network: mdb.NetworkAptos, Token: token.Symbol, Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, token, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
		t.Fatal(err)
	}
	return order
}

func assertAptosRegressionOrderSettled(t *testing.T, order *mdb.Orders) {
	t.Helper()
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" {
		t.Fatalf("payment not settled once: order=%+v err=%v", got, err)
	}
}

func TestAptosMintDepositDoesNotBlockLaterPayments(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	asset := state.tokens[0].ContractAddress
	order := seedAptosRegressionOrder(t, owner, state.tokens[0])
	if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
		t.Fatal(err)
	}
	var mint map[string]interface{}
	if err := json.Unmarshal(aptosDepositTransaction(t, owner, asset, 105), &mint); err != nil {
		t.Fatal(err)
	}
	mint["events"] = mint["events"].([]interface{})[1:] // valid mint_to: Deposit without Withdraw.
	mintBody, err := json.Marshal(mint)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateAptosLedgerContents(append(append([]byte{'['}, mintBody...), ']')); err != nil {
		t.Fatal(err)
	}
	p := &fakeAptosDepositProvider{head: 110, rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset, 105, 0, "3000000"), indexedDeposit(owner, asset, 106, 1, "3000000")}}, transactions: map[int64][]byte{105: mintBody, 106: aptosDepositTransaction(t, owner, asset, 106)}}
	for i := 0; i < 3; i++ {
		if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
			t.Fatal(err)
		}
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 110, Index: math.MaxInt64})
	if p.transactionCalls != 2 {
		t.Fatalf("unexpected RPC count %d", p.transactionCalls)
	}
	var receipts int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
	if receipts != 2 {
		t.Fatalf("mint and later payment receipts=%d, want 2", receipts)
	}
	assertAptosRegressionOrderSettled(t, order)
	// Replaying the page must use the original event keys and not credit twice.
	if err := saveAptosDepositCursor(owner, service.AptosEventPosition{Version: 100, Index: math.MaxInt64}); err != nil {
		t.Fatal(err)
	}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
	if receipts != 2 {
		t.Fatalf("duplicate receipts after replay: %d", receipts)
	}
}

type validatedRPCDepositProvider struct{ fakeAptosDepositProvider }

func (p *validatedRPCDepositProvider) Transaction(version int64, validate func([]byte) error) ([]byte, error) {
	return service.AptosGetTransactionByVersionValidated(version, validate)
}

func TestAptosIncompletePublicResponseUsesPaidFallback(t *testing.T) {
	for _, tc := range []struct {
		name, missing string
		invalidBackup bool
	}{
		{"missing_changes", "changes", false},
		{"missing_events", "events", false},
		{"all_nodes_invalid", "changes", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			resetPaidFallbackState(t)
			owner := seedAptosScannerChain(t, "0xa")[0]
			state, err := loadMoveWatchState(mdb.NetworkAptos)
			if err != nil {
				t.Fatal(err)
			}
			asset := state.tokens[0].ContractAddress
			order := seedAptosRegressionOrder(t, owner, state.tokens[0])
			if err := data.SavePaymentCursor("aptos:ledger", "100"); err != nil {
				t.Fatal(err)
			}
			full := aptosDepositTransaction(t, owner, asset, 105)
			var partial map[string]interface{}
			if err := json.Unmarshal(full, &partial); err != nil {
				t.Fatal(err)
			}
			delete(partial, tc.missing)
			var publicCalls, paidCalls atomic.Int32
			public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				publicCalls.Add(1)
				_ = json.NewEncoder(w).Encode(partial)
			}))
			defer public.Close()
			paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paidCalls.Add(1)
				if tc.invalidBackup {
					_ = json.NewEncoder(w).Encode(partial)
					return
				}
				_, _ = w.Write(full)
			}))
			defer paid.Close()
			for i, u := range []string{public.URL, paid.URL} {
				purpose := mdb.RpcNodePurposeGeneral
				if i == 1 {
					purpose = mdb.RpcNodePurposePaidFallback
				}
				if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeHttp, Url: u, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: purpose}).Error; err != nil {
					t.Fatal(err)
				}
			}
			p := &validatedRPCDepositProvider{fakeAptosDepositProvider{head: 110, rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset, 105, 1, "3000000")}}}}
			if tc.invalidBackup {
				if err := scanAptosDepositRound(context.Background(), p, state, 1); err == nil {
					t.Fatal("incomplete backups accepted")
				}
				assertDepositCursor(t, owner, service.AptosEventPosition{Version: 100, Index: math.MaxInt64})
				var receipts int64
				dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay || receipts != 0 {
					t.Fatalf("invalid response settled payment: receipts=%d err=%v", receipts, err)
				}
				if publicCalls.Load() != 1 || paidCalls.Load() != 1 {
					t.Fatal("did not try both endpoints")
				}
				return
			}
			for i := 0; i < 2; i++ {
				if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
					t.Fatal(err)
				}
			}
			assertDepositCursor(t, owner, service.AptosEventPosition{Version: 110, Index: math.MaxInt64})
			if publicCalls.Load() != 1 || paidCalls.Load() != 1 {
				t.Fatalf("unexpected calls public=%d paid=%d", publicCalls.Load(), paidCalls.Load())
			}
			var receipts int64
			dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
			if receipts != 1 {
				t.Fatalf("receipts=%d, want 1", receipts)
			}
			assertAptosRegressionOrderSettled(t, order)
		})
	}
}

func TestAptosBudgetDeferralPreservesBootstrapAndDeposits(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	owner := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	asset := state.tokens[0].ContractAddress
	viper.Set("rpc_paid_requests_per_minute", 1)
	data.RecordRpcRequest(mdb.RpcNode{Network: mdb.NetworkAptos, Purpose: mdb.RpcNodePurposePaidFallback}, "", 200)
	p := &fakeAptosDepositProvider{head: 1000, rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset, 1050, 1, "3000000")}}, transactions: map[int64][]byte{1050: aptosDepositTransaction(t, owner, asset, 1050)}}
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: -1, Index: math.MaxInt64})
	if p.queryCalls != 0 || p.transactionCalls != 0 {
		t.Fatal("deferred address queried")
	}
	order := seedAptosRegressionOrder(t, owner, state.tokens[0])
	p.transactions[1050] = aptosDepositTransaction(t, owner, asset, 1050)
	data.ResetRpcRoutingStatsForTest()
	p.head = 2000 // chain advanced while the soft budget deferred this new address.
	if err := scanAptosDepositRound(context.Background(), p, state, 1); err != nil {
		t.Fatal(err)
	}
	assertDepositCursor(t, owner, service.AptosEventPosition{Version: 2000, Index: math.MaxInt64})
	if p.transactionCalls != 1 {
		t.Fatalf("deposit reads=%d, want 1", p.transactionCalls)
	}
	var receipts int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts)
	if receipts != 1 {
		t.Fatalf("deposit lost: receipts=%d", receipts)
	}
	assertAptosRegressionOrderSettled(t, order)
}
