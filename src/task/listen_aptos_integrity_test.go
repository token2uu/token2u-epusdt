package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func assertAptosDurableCursor(t *testing.T, want string) {
	t.Helper()
	got, err := data.LoadPaymentCursor("aptos:ledger")
	if err != nil || got != want {
		t.Fatalf("durable cursor = %q, %v; want %q", got, err, want)
	}
}

func TestAptosIntegrityRejectsInvalidBatchWithoutAdvancing(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"empty", `[]`},
		{"null", `null`},
		{"object", `{}`},
		{"truncated_json", `[{"version":"100"}`},
		{"short", `[{"version":"100"},{"version":"101"}]`},
		{"extra", `[{"version":"100"},{"version":"101"},{"version":"102"},{"version":"103"}]`},
		{"wrong_start", `[{"version":"99"},{"version":"100"},{"version":"101"}]`},
		{"gap", `[{"version":"100"},{"version":"102"},{"version":"103"}]`},
		{"duplicate", `[{"version":"100"},{"version":"100"},{"version":"102"}]`},
		{"out_of_order", `[{"version":"101"},{"version":"100"},{"version":"102"}]`},
		{"missing_version", `[{}, {"version":"101"},{"version":"102"}]`},
		{"invalid_version", `[{"version":"100.5"},{"version":"101"},{"version":"102"}]`},
		{"overflow_version", `[{"version":"18446744073709551616"},{"version":"101"},{"version":"102"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			if err := data.SavePaymentCursor("aptos:ledger", "99"); err != nil {
				t.Fatal(err)
			}
			cursor := &aptosRuntimeCursor{persistent: true, initialized: true, lastSeenVersion: 99}
			provider := &fakeAptosProvider{bodiesByStart: map[int64][]byte{100: []byte(tc.body)}}
			catchup, err := processAptosLedgerRound(context.Background(), provider, moveWatchState{}, cursor, 102)
			if err == nil || !catchup || cursor.lastSeenVersion != 99 {
				t.Fatalf("invalid batch advanced: cursor=%d catchup=%v err=%v", cursor.lastSeenVersion, catchup, err)
			}
			assertAptosDurableCursor(t, "99")
		})
	}
}

func TestAptosIntegrityRetriesWholeBatchBeforeSettling(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	receive := seedAptosScannerChain(t, "0xa")[0]
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(10 * time.Minute)
	order := &mdb.Orders{TradeId: "integrity-payment", OrderId: "integrity-payment", Network: mdb.NetworkAptos, Token: "USDT",
		Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, *asset, []mdb.WalletAddress{{Address: receive}}, false); err != nil {
		t.Fatal(err)
	}
	var transactions, payment []json.RawMessage
	if err := json.Unmarshal(aptosLedgerBatch(100, 100), &transactions); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(aptosFungibleTransferBody(t, "0xabc175", 175, receive, asset.ContractAddress, "3000000"), &payment); err != nil {
		t.Fatal(err)
	}
	transactions[75] = payment[0]
	partial, err := json.Marshal(transactions[:76])
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeAptosProvider{bodiesByStart: map[int64][]byte{100: partial}}
	cursor := &aptosRuntimeCursor{persistent: true, initialized: true, lastSeenVersion: 99}
	if err := data.SavePaymentCursor("aptos:ledger", "99"); err != nil {
		t.Fatal(err)
	}
	if _, err := processAptosLedgerRound(context.Background(), provider, state, cursor, 199); err == nil {
		t.Fatal("partial batch accepted")
	}
	assertAptosDurableCursor(t, "99")
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusWaitPay || got.PaidAmount != 0 {
		t.Fatalf("payment processed before integrity validation: %+v, %v", got, err)
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("invalid batch wrote receipts: count=%d err=%v", count, err)
	}
	full, err := json.Marshal(transactions)
	if err != nil {
		t.Fatal(err)
	}
	provider.bodiesByStart[100] = full
	for i := 0; i < 2; i++ {
		// Re-read the same valid range to verify receipt deduplication as well.
		cursor.lastSeenVersion = 99
		if _, err := processAptosLedgerRound(context.Background(), provider, state, cursor, 199); err != nil {
			t.Fatal(err)
		}
	}
	assertAptosDurableCursor(t, "199")
	got, err = data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidAmount != 21 || got.PaidCryptoAmount != "3" || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("recovered payment not settled by actual amount: %+v, %v", got, err)
	}
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("replay did not deduplicate: count=%d err=%v", count, err)
	}
}

func TestAptosIntegrityStopsAtGapAndResumesAfterRestart(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	seedAptosScannerChain(t, "0xa")
	state, err := loadMoveWatchState(mdb.NetworkAptos)
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeAptosProvider{latest: 399, bodiesByStart: map[int64][]byte{200: []byte("[]")}}
	cursor := &aptosRuntimeCursor{persistent: true, initialized: true, lastSeenVersion: 99}
	if _, err := processAptosLedgerRound(context.Background(), provider, state, cursor, 399); err == nil {
		t.Fatal("missing middle batch accepted")
	}
	if cursor.lastSeenVersion != 199 {
		t.Fatalf("cursor crossed failed batch: %d", cursor.lastSeenVersion)
	}
	assertAptosDurableCursor(t, "199")
	delete(provider.bodiesByStart, 200)
	provider.calls = nil
	restarted := &aptosRuntimeCursor{persistent: true}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runAptosLedgerScanner(ctx, provider, restarted); err != nil {
		t.Fatal(err)
	}
	assertAptosDurableCursor(t, "399")
	if restarted.lastSeenVersion != 399 {
		t.Fatalf("restart failed to catch up: %d", restarted.lastSeenVersion)
	}
	calls := provider.rangeCalls()
	if len(calls) != 2 || calls[0].start != 200 || calls[1].start != 300 {
		t.Fatalf("restart did not resume at failed range: %+v", calls)
	}
}
