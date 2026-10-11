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

func TestAptosIntegrityContentRecovery(t *testing.T) {
	corruptions := map[string]func(map[string]interface{}){
		"missing_type":      func(tx map[string]interface{}) { delete(tx, "type") },
		"missing_success":   func(tx map[string]interface{}) { delete(tx, "success") },
		"null_success":      func(tx map[string]interface{}) { tx["success"] = nil },
		"string_success":    func(tx map[string]interface{}) { tx["success"] = "true" },
		"missing_hash":      func(tx map[string]interface{}) { delete(tx, "hash") },
		"empty_hash":        func(tx map[string]interface{}) { tx["hash"] = "" },
		"invalid_hash":      func(tx map[string]interface{}) { tx["hash"] = 123 },
		"missing_timestamp": func(tx map[string]interface{}) { delete(tx, "timestamp") },
		"invalid_timestamp": func(tx map[string]interface{}) { tx["timestamp"] = "broken" },
		"missing_events":    func(tx map[string]interface{}) { delete(tx, "events") },
		"null_events":       func(tx map[string]interface{}) { tx["events"] = nil },
		"invalid_events":    func(tx map[string]interface{}) { tx["events"] = map[string]interface{}{} },
		"missing_changes":   func(tx map[string]interface{}) { delete(tx, "changes") },
		"null_changes":      func(tx map[string]interface{}) { tx["changes"] = nil },
		"invalid_changes":   func(tx map[string]interface{}) { tx["changes"] = "broken" },
		"incomplete_event": func(tx map[string]interface{}) {
			delete(tx["events"].([]interface{})[1].(map[string]interface{}), "data")
		},
		"incomplete_amount": func(tx map[string]interface{}) {
			delete(tx["events"].([]interface{})[1].(map[string]interface{})["data"].(map[string]interface{}), "amount")
		},
		"incomplete_store": func(tx map[string]interface{}) {
			delete(tx["events"].([]interface{})[1].(map[string]interface{})["data"].(map[string]interface{}), "store")
		},
		"incomplete_resource": func(tx map[string]interface{}) {
			delete(tx["changes"].([]interface{})[1].(map[string]interface{}), "data")
		},
		"missing_metadata": func(tx map[string]interface{}) {
			delete(tx["changes"].([]interface{})[1].(map[string]interface{})["data"].(map[string]interface{})["data"].(map[string]interface{}), "metadata")
		},
		"missing_owner": func(tx map[string]interface{}) {
			delete(tx["changes"].([]interface{})[2].(map[string]interface{})["data"].(map[string]interface{})["data"].(map[string]interface{}), "owner")
		},
	}
	for name, corrupt := range corruptions {
		t.Run(name, func(t *testing.T) {
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
			o := &mdb.Orders{TradeId: "content-payment", OrderId: "content-payment", Network: mdb.NetworkAptos, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
			if err := data.CreateExclusivePayment(o, *asset, []mdb.WalletAddress{{Address: receive}}, false); err != nil {
				t.Fatal(err)
			}
			full := aptosFungibleTransferBody(t, "0xabc100", 100, receive, asset.ContractAddress, "3000000")
			var txs []map[string]interface{}
			if err := json.Unmarshal(full, &txs); err != nil {
				t.Fatal(err)
			}
			corrupt(txs[0])
			broken, err := json.Marshal(txs)
			if err != nil {
				t.Fatal(err)
			}
			provider := &fakeAptosProvider{bodiesByStart: map[int64][]byte{100: broken}}
			cursor := &aptosRuntimeCursor{persistent: true, initialized: true, lastSeenVersion: 99}
			if err := data.SavePaymentCursor("aptos:ledger", "99"); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if _, err := processAptosLedgerRound(context.Background(), provider, state, cursor, 100); err == nil {
					t.Fatal("incomplete content accepted")
				}
				assertAptosDurableCursor(t, "99")
			}
			got, err := data.GetOrderInfoByTradeId(o.TradeId)
			if err != nil || got.Status != mdb.StatusWaitPay {
				t.Fatalf("incomplete content settled: %+v %v", got, err)
			}
			var count int64
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("incomplete content consumed: %d %v", count, err)
			}
			provider.bodiesByStart[100] = full
			if _, err := processAptosLedgerRound(context.Background(), provider, state, cursor, 100); err != nil {
				t.Fatal(err)
			}
			assertAptosDurableCursor(t, "100")
			// Replay after recovery also uses the persisted receipt to deduplicate.
			cursor.lastSeenVersion = 99
			if _, err := processAptosLedgerRound(context.Background(), provider, state, cursor, 100); err != nil {
				t.Fatal(err)
			}
			got, err = data.GetOrderInfoByTradeId(o.TradeId)
			if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidAmount != 21 || got.PaidCryptoAmount != "3" || got.CallBackConfirm != mdb.CallBackConfirmNo {
				t.Fatalf("recovery failed: %+v %v", got, err)
			}
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
				t.Fatalf("incorrect receipt count: %d %v", count, err)
			}
		})
	}
}

func TestAptosIntegrityContentValidatedBeforeAnyPayment(t *testing.T) {
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
	expires := time.Now().Add(time.Minute)
	o := &mdb.Orders{TradeId: "batch-content", OrderId: "batch-content", Network: mdb.NetworkAptos, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(o, *asset, []mdb.WalletAddress{{Address: receive}}, false); err != nil {
		t.Fatal(err)
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(aptosFungibleTransferBody(t, "0xabc100", 100, receive, asset.ContractAddress, "3000000"), &rows); err != nil {
		t.Fatal(err)
	}
	rows = append(rows, json.RawMessage(`{"version":"101","type":"user_transaction","success":true}`))
	body, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeAptosProvider{bodiesByStart: map[int64][]byte{100: body}}
	cursor := &aptosRuntimeCursor{persistent: true, initialized: true, lastSeenVersion: 99}
	if err := data.SavePaymentCursor("aptos:ledger", "99"); err != nil {
		t.Fatal(err)
	}
	if _, err := processAptosLedgerRound(context.Background(), provider, state, cursor, 101); err == nil {
		t.Fatal("incomplete second transaction accepted")
	}
	assertAptosDurableCursor(t, "99")
	got, err := data.GetOrderInfoByTradeId(o.TradeId)
	if err != nil || got.Status != mdb.StatusWaitPay {
		t.Fatalf("first payment applied before batch validation: %+v %v", got, err)
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("incomplete batch wrote receipt: %d %v", count, err)
	}
}

func TestAptosIntegrityNonPaymentContent(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	provider := &fakeAptosProvider{bodiesByStart: map[int64][]byte{100: []byte(`[
		{"version":"100","type":"state_checkpoint_transaction"},
		{"version":"101","type":"block_metadata_transaction"},
		{"version":"102","type":"user_transaction","success":false},
		{"version":"103","type":"user_transaction","success":true,"hash":"0xabc","timestamp":"1700000000000000","events":[],"changes":[]}
	]`)}}
	cursor := &aptosRuntimeCursor{persistent: true, initialized: true, lastSeenVersion: 99}
	if _, err := processAptosLedgerRound(context.Background(), provider, moveWatchState{}, cursor, 103); err != nil {
		t.Fatal(err)
	}
	assertAptosDurableCursor(t, "103")
}
