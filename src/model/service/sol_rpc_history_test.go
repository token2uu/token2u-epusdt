package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func isolateSolHistoryBackoff(t *testing.T) func(time.Duration) {
	t.Helper()
	old := solHistoryBackoff
	now := time.Now()
	solHistoryBackoff = newSolanaHistoryBackoff()
	solHistoryBackoff.now = func() time.Time { return now }
	t.Cleanup(func() { solHistoryBackoff = old })
	return func(d time.Duration) { now = now.Add(d) }
}

func writeMissingSolHistory(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32020,"message":"Transaction not found"}}`))
}

func TestSolMissingHistoryUsesBackupWithoutPoisoningOtherQueries(t *testing.T) {
	var missingCalls, backupCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if req.Method == "getSignaturesForAddress" {
			var owner string
			var opts struct{ Until string }
			_ = json.Unmarshal(req.Params[0], &owner)
			_ = json.Unmarshal(req.Params[1], &opts)
			if owner == "blocked-owner" && opts.Until == "lost" {
				missingCalls.Add(1)
				// Some providers return JSON-RPC errors with HTTP 400.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				writeMissingSolHistory(w)
				return
			}
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":[]}`))
	}))
	defer primary.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupCalls.Add(1)
		var req struct{ Params []json.RawMessage }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		var opts struct{ Until, Before string }
		_ = json.Unmarshal(req.Params[1], &opts)
		if opts.Until != "lost" || opts.Before != "page-boundary" {
			t.Errorf("fallback changed pagination boundaries: %+v", opts)
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":[]}`))
	}))
	defer backup.Close()
	t.Cleanup(setupSolanaRPCNode(t, primary.URL))
	advance := isolateSolHistoryBackoff(t)
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcFailoverForTest)
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Url: backup.URL, Type: mdb.RpcNodeTypeHttp, Weight: 1, Enabled: true, Purpose: mdb.RpcNodePurposeGeneral, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	query := func(owner, until string) {
		t.Helper()
		if _, err := SolGetSignaturesForAddress(owner, 100, until, "page-boundary"); err != nil {
			t.Fatal(err)
		}
	}
	query("blocked-owner", "lost") // First failure must immediately fall back.
	if missingCalls.Load() != 1 || backupCalls.Load() != 1 {
		t.Fatalf("first query did not fall back: primary=%d backup=%d", missingCalls.Load(), backupCalls.Load())
	}
	query("healthy-owner", "lost")
	query("blocked-owner", "different-cursor")
	if _, err := SolRetryClient("getHealth", nil); err != nil {
		t.Fatal(err)
	}
	query("blocked-owner", "lost")
	query("blocked-owner", "lost")
	if missingCalls.Load() != 1 || backupCalls.Load() != 3 {
		t.Fatalf("unrelated success reset backoff: primary=%d backup=%d", missingCalls.Load(), backupCalls.Load())
	}
	advance(30 * time.Second)
	query("blocked-owner", "lost")
	if missingCalls.Load() != 2 || backupCalls.Load() != 4 {
		t.Fatalf("did not reprobe primary after cooldown: primary=%d backup=%d", missingCalls.Load(), backupCalls.Load())
	}
}

func TestSolMissingHistoryBackoffGrowsAndSuccessfulQueryResetsIt(t *testing.T) {
	for _, response := range []string{"rpc_error", "null_result"} {
		t.Run(response, func(t *testing.T) {
			advance := isolateSolHistoryBackoff(t)
			isolateSolThrottle(t, 1000)
			var calls atomic.Int32
			var recovered atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if !recovered.Load() {
					if response == "null_result" {
						_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":null}`))
					} else {
						writeMissingSolHistory(w)
					}
					return
				}
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"slot":123}}`))
			}))
			defer server.Close()
			query := func() error {
				_, err := solRetryClientWithURL(server.URL, "getTransaction", []interface{}{"missing-signature"})
				return err
			}
			for i, delay := range []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute, 15 * time.Minute} {
				if err := query(); !isSolanaHistoryUnavailable(err) {
					t.Fatalf("attempt %d: %v", i, err)
				}
				advance(delay - time.Millisecond)
				var deferred *solanaHistoryBackoffError
				if err := query(); !errors.As(err, &deferred) || !deferred.cached {
					t.Fatalf("attempt %d was not deferred until %s: %v", i, delay, err)
				}
				if response == "null_result" && !errors.Is(deferred, errSolanaTransactionUnavailable) {
					t.Fatal("cached null transaction lost its original error cause")
				}
				if calls.Load() != int32(i+1) {
					t.Fatalf("extra HTTP requests during cooldown: %d", calls.Load())
				}
				advance(time.Millisecond)
			}
			recovered.Store(true)
			if err := query(); err != nil {
				t.Fatalf("recovered query: %v", err)
			}
			recovered.Store(false)
			if err := query(); !isSolanaHistoryUnavailable(err) {
				t.Fatal(err)
			}
			advance(30 * time.Second)
			before := calls.Load()
			_ = query()
			if calls.Load() != before+1 {
				t.Fatal("successful query did not reset the delay to 30 seconds")
			}
		})
	}
}

func TestSolConcurrentMissingHistoryMakesOneHTTPRequest(t *testing.T) {
	isolateSolHistoryBackoff(t)
	isolateSolThrottle(t, 1000)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		writeMissingSolHistory(w)
	}))
	defer server.Close()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := solRetryClientWithURL(server.URL, "getTransaction", []interface{}{"missing"})
			if !isSolanaHistoryUnavailable(err) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent repeated query made %d HTTP requests", calls.Load())
	}
}

func TestSolHistoryBackoffDoesNotBlockNewCredentials(t *testing.T) {
	isolateSolHistoryBackoff(t)
	isolateSolThrottle(t, 1000)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "new-credential" {
			writeMissingSolHistory(w)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":[]}`))
	}))
	defer server.Close()
	_, err := solRetryClientWithURL(server.URL, "getSignaturesForAddress", []interface{}{"owner"})
	if !isSolanaHistoryUnavailable(err) {
		t.Fatal(err)
	}
	_, err = solRetryClientWithURLHeaders(server.URL, "getSignaturesForAddress", []interface{}{"owner"}, http.Header{"Authorization": {"new-credential"}})
	if err != nil || calls.Load() != 2 {
		t.Fatalf("updated credential blocked: calls=%d err=%v", calls.Load(), err)
	}
}

func TestSolMissingHistoryPreservesCursorAndSettlesAfterRecovery(t *testing.T) {
	for _, tc := range []struct {
		name            string
		withBackup      bool
		nullTransaction bool
	}{
		{name: "wait_for_history_recovery"},
		{name: "immediate_backup_recovery", withBackup: true},
		{name: "wait_for_null_transaction_recovery", nullTransaction: true},
		{name: "null_transaction_backup_recovery", withBackup: true, nullTransaction: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledger := setupSolCursorLedger(t)
			advance := isolateSolHistoryBackoff(t)
			data.ResetRpcEndpointPolicyForTest()
			t.Cleanup(data.ResetRpcEndpointPolicyForTest)
			data.ResetRpcFailoverForTest()
			t.Cleanup(data.ResetRpcFailoverForTest)
			ledger.add("A", 1, "")
			scanSolCursor()
			assertSolCursor(t, ledger, "A")
			order := solCursorOrder(t)
			ledger.add("B", 2, "3000000")

			var unavailable atomic.Bool
			unavailable.Store(true)
			var blockedCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				var req struct {
					Method string
					Params []json.RawMessage
				}
				if err := json.Unmarshal(body, &req); err != nil {
					t.Error(err)
					return
				}
				var target string
				_ = json.Unmarshal(req.Params[0], &target)
				missingSignature := !tc.nullTransaction && req.Method == "getSignaturesForAddress" && target == ledger.ata
				missingTransaction := tc.nullTransaction && req.Method == "getTransaction"
				if unavailable.Load() && (missingSignature || missingTransaction) {
					// Count retries of the original query separately from the
					// extra before=B query that confirms the range is exhausted.
					var opts struct{ Before string }
					if missingSignature && len(req.Params) == 2 {
						_ = json.Unmarshal(req.Params[1], &opts)
					}
					if opts.Before == "" {
						blockedCalls.Add(1)
					}
					if tc.nullTransaction {
						_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":null}`))
					} else {
						writeMissingSolHistory(w)
					}
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				ledger.ServeHTTP(w, r)
			}))
			defer primary.Close()
			nodes, err := data.ListRpcNodes(mdb.NetworkSolana)
			if err != nil || len(nodes) != 1 {
				t.Fatalf("initial nodes: %v %v", nodes, err)
			}
			originalURL := nodes[0].Url
			if err := data.UpdateRpcNodeFields(nodes[0].ID, map[string]interface{}{"url": primary.URL, "weight": 100}); err != nil {
				t.Fatal(err)
			}
			if tc.withBackup {
				if err := data.CreateRpcNode(&mdb.RpcNode{Network: mdb.NetworkSolana, Url: originalURL, Type: mdb.RpcNodeTypeHttp, Weight: 1, Enabled: true, Purpose: mdb.RpcNodePurposeGeneral, Status: mdb.RpcNodeStatusOk}); err != nil {
					t.Fatal(err)
				}
				// Match health status so primary weight controls selection.
				if err := data.UpdateRpcNodeFields(nodes[0].ID, map[string]interface{}{"status": mdb.RpcNodeStatusOk}); err != nil {
					t.Fatal(err)
				}
			}
			scanSolCursor()
			if !tc.withBackup {
				assertSolCursor(t, ledger, "A")
				scanSolCursor() // Owner query succeeds but must not reset ATA backoff.
				assertSolCursor(t, ledger, "A")
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay {
					t.Fatalf("unresolved history settled payment: %+v %v", got, err)
				}
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
					t.Fatalf("unresolved history wrote receipts: %d %v", count, err)
				}
				if blockedCalls.Load() != 1 {
					t.Fatalf("unchanged failed cursor retried during backoff: %d", blockedCalls.Load())
				}
				unavailable.Store(false)
				advance(30 * time.Second)
				scanSolCursor()
			} else if blockedCalls.Load() != 1 {
				t.Fatalf("did not attempt primary history query: %d", blockedCalls.Load())
			}
			assertSolCursor(t, ledger, "B")
			assertSolCursorSettlement(t, order)
			scanSolCursor()
			assertSolCursorSettlement(t, order) // No duplicate settlement after replay.
		})
	}
}
