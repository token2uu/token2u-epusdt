package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

const solCursorTestOwner = "2uFTf9TZ8gd7Kg6hkb79TxfaeNpaAgpJ8uVHguv2Yweu"

// Serve a newest-first ledger with the RPC's exclusive before/until bounds.
// Tests assert durable receipts and order transitions, not just RPC arguments.
type solCursorLedger struct {
	mu             sync.Mutex
	ata            string
	signatures     []solSignatureResult
	transactions   map[string]interface{}
	failPagination bool
}

func (l *solCursorLedger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var req struct {
		Method string
		Params []json.RawMessage
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Params) == 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var target string
	if err := json.Unmarshal(req.Params[0], &target); err != nil {
		http.Error(w, "invalid target", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	var result interface{}
	switch req.Method {
	case "getSignaturesForAddress":
		var opts struct {
			Before, Until string
			Limit         int
		}
		if len(req.Params) != 2 || json.Unmarshal(req.Params[1], &opts) != nil || opts.Limit <= 0 {
			http.Error(w, "invalid options", http.StatusBadRequest)
			return
		}
		if l.failPagination && opts.Before != "" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": map[string]interface{}{"code": -32000, "message": "temporary page failure"}})
			return
		}
		batch := make([]solSignatureResult, 0)
		if target == l.ata {
			start := 0
			if opts.Before != "" {
				start = len(l.signatures)
				for i, sig := range l.signatures {
					if sig.Signature == opts.Before {
						start = i + 1
						break
					}
				}
			}
			for _, sig := range l.signatures[start:] {
				if sig.Signature == opts.Until || len(batch) == opts.Limit {
					break
				}
				batch = append(batch, sig)
			}
		}
		result = batch
	case "getTransaction":
		result = l.transactions[target]
	default:
		http.Error(w, "unexpected method", http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": result})
}

func setupSolCursorLedger(t *testing.T) *solCursorLedger {
	t.Helper()
	ata, err := FindATAAddress(solCursorTestOwner, USDT_Mint)
	if err != nil {
		t.Fatal(err)
	}
	l := &solCursorLedger{ata: ata, transactions: make(map[string]interface{})}
	server := httptest.NewServer(l)
	t.Cleanup(server.Close)
	t.Cleanup(setupSolanaRPCNode(t, server.URL))
	clearProcessedSignatures()
	t.Cleanup(clearProcessedSignatures)
	if err := dao.Mdb.Create(&mdb.ChainToken{Network: mdb.NetworkSolana, Symbol: "USDT", ContractAddress: USDT_Mint, Decimals: 6, Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	return l
}

func (l *solCursorLedger) add(sig string, slot uint64, amount string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now().Unix()
	entry := solSignatureResult{Signature: sig, Slot: slot, BlockTime: &now}
	if amount == "" {
		entry.Err = "failed transaction"
	} else {
		l.transactions[sig] = map[string]interface{}{
			"blockTime": now, "meta": map[string]interface{}{"err": nil},
			"transaction": map[string]interface{}{"message": map[string]interface{}{"instructions": []interface{}{
				map[string]interface{}{"programId": TokenProgramID, "parsed": map[string]interface{}{
					"type": "transferChecked", "info": map[string]interface{}{
						"source": "source-ata", "destination": l.ata, "mint": USDT_Mint,
						"tokenAmount": map[string]interface{}{"amount": amount, "decimals": 6},
					},
				}},
			}}},
		}
	}
	l.signatures = append([]solSignatureResult{entry}, l.signatures...)
}

func solCursorOrder(t *testing.T) *mdb.Orders {
	t.Helper()
	expires := time.Now().Add(10 * time.Minute)
	o := &mdb.Orders{TradeId: "cursor-order", OrderId: "cursor-order", Network: mdb.NetworkSolana, Token: "USDT",
		Amount: 35, ActualAmount: 5, Currency: "CNY", PaymentType: mdb.PaymentTypeEpay, Status: mdb.StatusWaitPay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(o, mdb.ChainToken{ContractAddress: USDT_Mint, Decimals: 6}, []mdb.WalletAddress{{Address: solCursorTestOwner}}, false); err != nil {
		t.Fatal(err)
	}
	// Put assignment in an earlier second so this test is independent of the
	// separate timestamp-resolution issue in payment matching.
	if err := dao.Mdb.Model(o).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	return o
}

func scanSolCursor() {
	var wg sync.WaitGroup
	wg.Add(1)
	SolCallBack(solCursorTestOwner, &wg)
	wg.Wait()
}

func assertSolCursor(t *testing.T, l *solCursorLedger, want string) {
	t.Helper()
	got, err := data.LoadPaymentCursor("solana:signatures:" + l.ata)
	if err != nil || got != want {
		t.Fatalf("persisted cursor = %q, %v; want %q", got, err, want)
	}
}

func assertSolCursorSettlement(t *testing.T, o *mdb.Orders) {
	t.Helper()
	got, err := data.GetOrderInfoByTradeId(o.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || got.PaidAmount != 21 || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("new payment not settled and queued for callback: %+v", got)
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("receipt count = %d, %v; want 1", count, err)
	}
}

func TestSolCursorFindsNewPaymentAndDeduplicatesReplay(t *testing.T) {
	l := setupSolCursorLedger(t)
	l.add("A", 1, "")
	scanSolCursor()
	assertSolCursor(t, l, "A")
	o := solCursorOrder(t)
	l.add("B", 2, "3000000")
	clearProcessedSignatures() // No in-memory state from the preceding scan.
	scanSolCursor()
	assertSolCursor(t, l, "B")
	assertSolCursorSettlement(t, o)

	// An unchanged ledger must neither move the cursor backwards nor settle twice.
	scanSolCursor()
	assertSolCursor(t, l, "B")
	assertSolCursorSettlement(t, o)
	// An older persisted cursor (as left by the bug) safely replays receipts.
	if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
		t.Fatal(err)
	}
	clearProcessedSignatures()
	scanSolCursor()
	assertSolCursor(t, l, "B")
	assertSolCursorSettlement(t, o)
}

func TestSolCursorPaginationRetriesBeforeAdvancing(t *testing.T) {
	l := setupSolCursorLedger(t)
	l.add("A", 1, "")
	if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
		t.Fatal(err)
	}
	o := solCursorOrder(t)
	l.add("payment-on-second-page", 2, "3000000")
	for i := 0; i < 1000; i++ {
		l.add(fmt.Sprintf("new-%04d", i), uint64(i+3), "")
	}
	l.mu.Lock()
	l.failPagination = true
	l.mu.Unlock()
	scanSolCursor()
	assertSolCursor(t, l, "A")
	got, err := data.GetOrderInfoByTradeId(o.TradeId)
	if err != nil || got.Status != mdb.StatusWaitPay {
		t.Fatalf("failed page unexpectedly settled order: %+v, %v", got, err)
	}
	l.mu.Lock()
	l.failPagination = false
	l.mu.Unlock()
	clearProcessedSignatures()
	scanSolCursor()
	assertSolCursor(t, l, "new-0999")
	assertSolCursorSettlement(t, o)
}

func TestSolCursorCatchupLeavesExpiredPaymentForManualReview(t *testing.T) {
	l := setupSolCursorLedger(t)
	l.add("A", 1, "")
	if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
		t.Fatal(err)
	}
	o := solCursorOrder(t)
	if err := dao.Mdb.Model(o).Update("payment_expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	l.add("late-payment", 2, "3000000")
	scanSolCursor()
	assertSolCursor(t, l, "late-payment")
	got, err := data.GetOrderInfoByTradeId(o.TradeId)
	if err != nil || got.Status != mdb.StatusExpired || got.PaidAmount != 0 {
		t.Fatalf("catchup must not reopen expired order: %+v, %v", got, err)
	}
	var receipt mdb.PaymentReceipt
	if err := dao.Mdb.Where("tx_hash = ?", "late-payment").First(&receipt).Error; err != nil {
		t.Fatal(err)
	}
	if receipt.Status != "exception" || receipt.Reason != "late_payment" || receipt.TradeID != o.TradeId {
		t.Fatalf("late receipt not available for manual review: %+v", receipt)
	}
}

func TestSolCursorIncompleteDetailRetries(t *testing.T) {
	tests := []struct {
		name        string
		breakDetail func(result, message, meta, info map[string]interface{})
	}{
		{"missing_transaction", func(r, m, meta, info map[string]interface{}) { delete(r, "transaction") }},
		{"missing_instructions", func(r, m, meta, info map[string]interface{}) { delete(m, "instructions") }},
		{"null_instructions", func(r, m, meta, info map[string]interface{}) { m["instructions"] = nil }},
		{"invalid_instructions", func(r, m, meta, info map[string]interface{}) { m["instructions"] = "truncated" }},
		{"missing_meta", func(r, m, meta, info map[string]interface{}) { delete(r, "meta") }},
		{"null_meta", func(r, m, meta, info map[string]interface{}) { r["meta"] = nil }},
		{"missing_status", func(r, m, meta, info map[string]interface{}) { delete(meta, "err") }},
		{"invalid_inner_instructions", func(r, m, meta, info map[string]interface{}) { meta["innerInstructions"] = map[string]interface{}{} }},
		{"missing_inner_array", func(r, m, meta, info map[string]interface{}) {
			meta["innerInstructions"] = []interface{}{map[string]interface{}{"index": 0}}
		}},
		{"missing_destination", func(r, m, meta, info map[string]interface{}) { delete(info, "destination") }},
		{"missing_mint", func(r, m, meta, info map[string]interface{}) { delete(info, "mint") }},
		{"missing_decimals", func(r, m, meta, info map[string]interface{}) {
			delete(info["tokenAmount"].(map[string]interface{}), "decimals")
		}},
		{"invalid_amount", func(r, m, meta, info map[string]interface{}) {
			info["tokenAmount"].(map[string]interface{})["amount"] = "invalid"
		}},
		{"unresolved_spl_mint", func(r, m, meta, info map[string]interface{}) {
			m["instructions"].([]interface{})[0].(map[string]interface{})["parsed"].(map[string]interface{})["type"] = "transfer"
			info["amount"] = "3000000" // No token balances: the parser cannot resolve the mint.
		}},
		{"invalid_unchecked_destination", func(r, m, meta, info map[string]interface{}) {
			m["instructions"].([]interface{})[0].(map[string]interface{})["parsed"].(map[string]interface{})["type"] = "transfer"
			info["amount"] = "3000000"
			info["destination"] = "not-a-public-key"
		}},
		{"valid_transfer_then_broken_instruction", func(r, m, meta, info map[string]interface{}) {
			m["instructions"] = append(m["instructions"].([]interface{}), map[string]interface{}{})
		}},
		{"valid_transfer_then_broken_cpi", func(r, m, meta, info map[string]interface{}) {
			meta["innerInstructions"] = []interface{}{map[string]interface{}{"index": 0, "instructions": []interface{}{map[string]interface{}{
				"programId": TokenProgramID, "parsed": map[string]interface{}{"type": "transferChecked", "info": map[string]interface{}{}},
			}}}}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := setupSolCursorLedger(t)
			l.add("A", 1, "")
			if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
				t.Fatal(err)
			}
			o := solCursorOrder(t)
			l.add("B", 2, "3000000")
			full := l.transactions["B"]
			encoded, err := json.Marshal(full)
			if err != nil {
				t.Fatal(err)
			}
			var broken map[string]interface{}
			if err := json.Unmarshal(encoded, &broken); err != nil {
				t.Fatal(err)
			}
			message := broken["transaction"].(map[string]interface{})["message"].(map[string]interface{})
			info := message["instructions"].([]interface{})[0].(map[string]interface{})["parsed"].(map[string]interface{})["info"].(map[string]interface{})
			tc.breakDetail(broken, message, broken["meta"].(map[string]interface{}), info)
			l.transactions["B"] = broken
			// A newer, definitively failed signature must not move the cursor
			// past the incomplete payment, even across repeated scans.
			l.add("C", 3, "")
			for attempt := 0; attempt < 2; attempt++ {
				scanSolCursor()
				assertSolCursor(t, l, "A")
				got, err := data.GetOrderInfoByTradeId(o.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay || got.PaidAmount != 0 {
					t.Fatalf("partial payment applied: %+v %v", got, err)
				}
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
					t.Fatalf("unexpected receipt count=%d err=%v", count, err)
				}
				if _, cached := gProcessedSignatures.Load(solCursorTestOwner + ":B"); cached {
					t.Fatal("incomplete signature cached as processed")
				}
			}
			l.transactions["B"] = full
			clearProcessedSignatures() // Simulate restart using only persisted progress.
			scanSolCursor()
			assertSolCursor(t, l, "C")
			assertSolCursorSettlement(t, o)
			scanSolCursor()
			assertSolCursorSettlement(t, o)
		})
	}
}

func TestSolCursorValidOptionalMetadataAndCPI(t *testing.T) {
	for _, variant := range []string{"omitted_inner", "null_inner", "empty_inner", "cpi"} {
		t.Run(variant, func(t *testing.T) {
			l := setupSolCursorLedger(t)
			o := solCursorOrder(t)
			l.add("B", 2, "3000000")
			detail := l.transactions["B"].(map[string]interface{})
			meta := detail["meta"].(map[string]interface{})
			message := detail["transaction"].(map[string]interface{})["message"].(map[string]interface{})
			switch variant {
			case "null_inner":
				meta["innerInstructions"] = nil
			case "empty_inner":
				meta["innerInstructions"] = []interface{}{}
			case "cpi":
				meta["innerInstructions"] = []interface{}{map[string]interface{}{"index": 0, "instructions": message["instructions"]}}
				message["instructions"] = []interface{}{map[string]interface{}{"programId": "custom-program", "data": "abc", "accounts": []interface{}{}}}
			}
			// A successful non-payment transaction must not stall scanning.
			l.add("C", 3, "1")
			l.transactions["C"] = map[string]interface{}{"meta": map[string]interface{}{"err": nil}, "transaction": map[string]interface{}{"message": map[string]interface{}{"instructions": []interface{}{
				map[string]interface{}{"programId": "memo-program", "parsed": "a memo"},
			}}}}
			scanSolCursor()
			assertSolCursor(t, l, "C")
			assertSolCursorSettlement(t, o)
			var receipt mdb.PaymentReceipt
			if err := dao.Mdb.First(&receipt).Error; err != nil {
				t.Fatal(err)
			}
			wantEvent := "instruction:0"
			if variant == "cpi" {
				wantEvent = "instruction:1"
			}
			if receipt.EventID != wantEvent {
				t.Fatalf("event identity changed: %q", receipt.EventID)
			}
		})
	}
}

func TestSolCursorFirstScanFailureKeepsInitialLookback(t *testing.T) {
	l := setupSolCursorLedger(t)
	o := solCursorOrder(t)
	if err := dao.Mdb.Model(o).Update("assigned_at_ms", time.Now().Add(-2*time.Hour).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	l.add("B", 2, "3000000")
	oldTime := time.Now().Add(-time.Hour).Unix()
	l.signatures[0].BlockTime = &oldTime
	full := l.transactions["B"].(map[string]interface{})
	full["blockTime"] = oldTime
	l.transactions["B"] = map[string]interface{}{"meta": map[string]interface{}{"err": nil}}
	viper.Set("order_expiration_time", 120)
	scanSolCursor()
	assertSolCursor(t, l, "")

	// Shrink the moving lookback to model a retry after the initial window
	// has passed, without sleeping. The persisted starting boundary must win.
	viper.Set("order_expiration_time", 10)
	l.transactions["B"] = full
	clearProcessedSignatures()
	scanSolCursor()
	assertSolCursor(t, l, "B")
	assertSolCursorSettlement(t, o)
}
