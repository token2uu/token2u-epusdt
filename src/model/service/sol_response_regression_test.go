package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/gagliardetto/solana-go"
)

func TestSolNullHistoryDoesNotRouteHealthyQueriesToPaid(t *testing.T) {
	var missingCalls, healthyCalls, paidCalls atomic.Int32
	var recovered atomic.Bool
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if req.Method == "getTransaction" {
			var sig string
			_ = json.Unmarshal(req.Params[0], &sig)
			if sig == "missing" {
				missingCalls.Add(1)
				if !recovered.Load() {
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":null}`))
					return
				}
			} else {
				healthyCalls.Add(1)
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"slot":123}}`))
			return
		}
		healthyCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"ok"}`))
	}))
	defer primary.Close()
	t.Cleanup(setupSolanaRPCNode(t, primary.URL))
	isolateSolThrottle(t, 1000)
	advance := isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	t.Cleanup(data.ResetRpcFailoverForTest)
	paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paidCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"slot":123}}`))
	}))
	defer paid.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: paid.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposePaidFallback}).Error; err != nil {
		t.Fatal(err)
	}
	query := func(sig string) {
		t.Helper()
		if _, err := SolRetryClient("getTransaction", []interface{}{sig}); err != nil {
			t.Fatal(err)
		}
	}
	query("missing")
	query("other")
	for i := 0; i < 5; i++ {
		if _, err := SolRetryClient("getHealth", nil); err != nil {
			t.Fatal(err)
		}
	}
	query("missing") // Only this query should remain backed off on the primary.
	if healthyCalls.Load() != 6 || missingCalls.Load() != 1 || paidCalls.Load() != 2 {
		t.Fatalf("null history affected unrelated requests or lost its backoff: healthy=%d missing=%d paid=%d", healthyCalls.Load(), missingCalls.Load(), paidCalls.Load())
	}
	recovered.Store(true)
	advance(30 * time.Second)
	query("missing")
	query("missing")
	if missingCalls.Load() != 3 || paidCalls.Load() != 2 {
		t.Fatalf("recovered query did not return to public node: missing=%d paid=%d", missingCalls.Load(), paidCalls.Load())
	}
}

func TestSolAmbiguousSignatureResponseFallbackAndRecovery(t *testing.T) {
	for _, variant := range []string{"duplicate_err", "case_err", "escaped_err", "case_slot", "case_block_time", "duplicate_result"} {
		for _, backupHealthy := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/backup_%t", variant, backupHealthy), func(t *testing.T) {
				l := setupSolCursorLedger(t)
				isolateSolThrottle(t, 1000)
				isolateSolHistoryBackoff(t)
				data.ResetRpcEndpointPolicyForTest()
				data.ResetRpcFailoverForTest()
				t.Cleanup(data.ResetRpcEndpointPolicyForTest)
				t.Cleanup(data.ResetRpcFailoverForTest)
				l.add("A", 1, "")
				if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
					t.Fatal(err)
				}
				order := solCursorOrder(t)
				sig := (solana.Signature{1}).String()
				l.add(sig, 2, "3000000")
				extra := `,"err":{"InstructionError":[0,"InvalidArgument"]}`
				switch variant {
				case "case_err":
					extra = `,"Err":{"InstructionError":[0,"InvalidArgument"]}`
				case "escaped_err":
					extra = `,"\u0065rr":{"InstructionError":[0,"InvalidArgument"]}`
				case "case_slot":
					extra = `,"Slot":0`
				case "case_block_time":
					extra = `,"BlockTime":1`
				case "duplicate_result":
					extra = ""
				}
				broken := fmt.Sprintf(`{"jsonrpc":"2.0","result":[{"signature":%q,"slot":2,"blockTime":%d,"err":null%s}]}`, sig, time.Now().Unix(), extra)
				if variant == "duplicate_result" {
					broken = `{"result":[],"result":` + broken[len(`{"jsonrpc":"2.0","result":`):]
				}
				var recovered atomic.Bool
				var paidCalls atomic.Int32
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
					if req.Method == "getSignaturesForAddress" && target == l.ata && !recovered.Load() {
						_, _ = w.Write([]byte(broken))
						return
					}
					r.Body = io.NopCloser(bytes.NewReader(body))
					l.ServeHTTP(w, r)
				}))
				defer primary.Close()
				if err := dao.Mdb.Model(&mdb.RpcNode{}).Where("network = ?", mdb.NetworkSolana).Update("url", primary.URL).Error; err != nil {
					t.Fatal(err)
				}
				paid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					paidCalls.Add(1)
					if !backupHealthy {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					l.ServeHTTP(w, r)
				}))
				defer paid.Close()
				if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: paid.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposePaidFallback}).Error; err != nil {
					t.Fatal(err)
				}
				err := ScanSolanaAddress(solCursorTestOwner)
				if !backupHealthy {
					if err == nil {
						t.Fatal("ambiguous response advanced a scan while backup was unavailable")
					}
					assertSolCursor(t, l, "A")
					got, loadErr := data.GetOrderInfoByTradeId(order.TradeId)
					var receipts int64
					if countErr := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; countErr != nil {
						t.Fatal(countErr)
					}
					if loadErr != nil || got.Status != mdb.StatusWaitPay || receipts != 0 {
						t.Fatalf("invalid response changed payment: order=%+v receipts=%d err=%v", got, receipts, loadErr)
					}
					recovered.Store(true)
					data.ResetRpcEndpointPolicyForTest()
					data.ResetRpcFailoverForTest()
					err = ScanSolanaAddress(solCursorTestOwner)
				}
				if err != nil {
					t.Fatal(err)
				}
				assertSolCursor(t, l, sig)
				assertSolCursorSettlement(t, order)
				if paidCalls.Load() == 0 {
					t.Fatal("ambiguous response did not trigger fallback")
				}
				recovered.Store(true)
				clearProcessedSignatures()
				if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
					t.Fatal(err)
				}
				assertSolCursorSettlement(t, order)
			})
		}
	}
}
