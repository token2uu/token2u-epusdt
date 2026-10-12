package service

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestSolShortPagesPreserveDeposits(t *testing.T) {
	for _, tc := range []struct {
		name        string
		bootstrap   bool
		failPage    bool
		repeatPage  bool
		backupFails bool
	}{
		{name: "public_short_pages"},
		{name: "initial_scan_short_pages", bootstrap: true},
		{name: "later_page_failure_uses_backup", failPage: true},
		{name: "repeated_page_uses_backup", repeatPage: true},
		{name: "all_nodes_fail_then_recover", failPage: true, backupFails: true},
		{name: "all_nodes_repeat_then_recover", repeatPage: true, backupFails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledger := setupSolCursorLedger(t)
			isolateSolThrottle(t, 1000)
			isolateSolHistoryBackoff(t)
			data.ResetRpcEndpointPolicyForTest()
			data.ResetRpcFailoverForTest()
			t.Cleanup(data.ResetRpcEndpointPolicyForTest)
			t.Cleanup(data.ResetRpcFailoverForTest)
			ledger.add("A", 1, "")
			initialCursor := ""
			if !tc.bootstrap {
				initialCursor = "A"
				if err := data.SavePaymentCursor("solana:signatures:"+ledger.ata, initialCursor); err != nil {
					t.Fatal(err)
				}
			}
			order := solCursorOrder(t)
			ledger.add("B", 2, "3000000")
			ledger.add("C", 3, "")
			ledger.mu.Lock()
			ledger.pageLimit = 1
			ledger.failPagination = tc.failPage
			ledger.repeatPage = tc.repeatPage
			backupLedger := &solCursorLedger{ata: ledger.ata, signatures: ledger.signatures, transactions: ledger.transactions}
			if tc.backupFails {
				backupLedger.failPagination = tc.failPage
				backupLedger.repeatPage = tc.repeatPage
				backupLedger.pageLimit = 1
			}
			ledger.mu.Unlock()
			var paidCalls atomic.Int32
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paidCalls.Add(1)
				backupLedger.ServeHTTP(w, r)
			}))
			t.Cleanup(backup.Close)
			if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: backup.URL,
				Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
				t.Fatal(err)
			}

			err := ScanSolanaAddress(solCursorTestOwner)
			if tc.backupFails {
				if err == nil {
					t.Fatal("incomplete pagination reported success")
				}
				assertSolCursor(t, ledger, initialCursor)
				var receipts int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != 0 {
					t.Fatalf("incomplete scan wrote receipts: %d, %v", receipts, err)
				}
				ledger.mu.Lock()
				ledger.failPagination = false
				ledger.repeatPage = false
				ledger.mu.Unlock()
				data.ResetRpcEndpointPolicyForTest()
				data.ResetRpcFailoverForTest()
				clearProcessedSignatures()
				if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			assertSolCursor(t, ledger, "C")
			assertSolCursorSettlement(t, order)
			if (tc.failPage || tc.repeatPage) != (paidCalls.Load() > 0) {
				t.Fatalf("unexpected paid fallback calls: %d", paidCalls.Load())
			}

			// Provider recovery and an empty in-memory cache must not duplicate
			// the receipt or uncover a deposit skipped by the short-page scan.
			ledger.mu.Lock()
			ledger.pageLimit = 0
			ledger.failPagination = false
			ledger.repeatPage = false
			ledger.mu.Unlock()
			data.ResetRpcEndpointPolicyForTest()
			data.ResetRpcFailoverForTest()
			clearProcessedSignatures()
			if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
				t.Fatal(err)
			}
			assertSolCursor(t, ledger, "C")
			assertSolCursorSettlement(t, order)
		})
	}
}
