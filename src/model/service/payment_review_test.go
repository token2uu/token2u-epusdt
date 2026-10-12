package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestPaidSolanaReviewDiscoversPaymentMissingFromHealthyPublicNode(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	order := solCursorOrder(t)
	expires := time.Now().Add(time.Minute)
	dao.Mdb.Model(order).Update("payment_expires_at", expires)
	// The healthy public endpoint returns an empty history. A successful empty
	// response must not prevent the independent paid near-deadline review.
	var public, paid atomic.Int32
	publicServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		public.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":[]}`))
	}))
	defer publicServer.Close()
	dao.Mdb.Model(&mdb.RpcNode{}).Where("network = ?", mdb.NetworkSolana).Update("url", publicServer.URL)
	l.add("paid-evidence", 2, "3000000")
	paidServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { paid.Add(1); l.ServeHTTP(w, r) }))
	defer paidServer.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: paidServer.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	if got, _ := data.GetOrderInfoByTradeId(order.TradeId); got.Status != mdb.StatusWaitPay || paid.Load() != 0 {
		t.Fatal("normal scan must remain public")
	}
	ctx, cancel := context.WithTimeout(WithPaidPaymentReview(context.Background()), time.Minute)
	defer cancel()
	if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	got, _ := data.GetOrderInfoByTradeId(order.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || paid.Load() == 0 {
		t.Fatalf("review missed payment: %+v paid=%d", got, paid.Load())
	}
	publicBefore := public.Load()
	if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	if public.Load() != publicBefore {
		t.Fatal("paid review hit public node")
	}
	var n int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&n)
	if n != 1 {
		t.Fatalf("duplicate receipts=%d", n)
	}
	if cursor, err := data.LoadPaymentCursor("solana:signatures:" + l.ata); err != nil || cursor != "" {
		t.Fatalf("review changed ordinary cursor: %s %v", cursor, err)
	}
	if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
		t.Fatal(err)
	}
	if public.Load() <= publicBefore {
		t.Fatal("review permanently moved normal scans to paid")
	}
	stats := data.SnapshotRpcRoutingStats()[mdb.NetworkSolana]
	if stats.FallbackReasons["payment_deadline"] == 0 {
		t.Fatal("deadline review not accounted")
	}
}

func TestPaidReviewCancellationDoesNotConsumeScanCursor(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer server.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(WithPaidPaymentReview(context.Background()), 50*time.Millisecond)
	defer cancel()
	if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err == nil {
		t.Fatal("cancelled review succeeded")
	}
	if cursor, err := data.LoadPaymentCursor("solana:payment_review:" + l.ata); err != nil || cursor != "" {
		t.Fatalf("cancelled review consumed cursor: %s %v", cursor, err)
	}
}

func TestPaidReviewDoesNotUseOrdinaryNodeWhenPaidIsMissing(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	ctx := WithPaidPaymentReview(context.Background())
	if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err == nil {
		t.Fatal("review succeeded without paid node")
	}
	if cursor, err := data.LoadPaymentCursor("solana:payment_review:" + l.ata); err != nil || cursor != "" {
		t.Fatalf("missing paid node consumed cursor: %s %v", cursor, err)
	}
	// The ordinary path can still run while the paid review is unavailable.
	if _, err := solRetryClientValidated("getSignaturesForAddress", []interface{}{l.ata, map[string]interface{}{"limit": 1}}, func(body []byte) error { var v interface{}; return json.Unmarshal(body, &v) }); err != nil {
		t.Fatal(err)
	}
}

func TestObservedPaymentReplaysRecoverableExclusiveReceipt(t *testing.T) {
	setupSolCursorLedger(t)
	order := solCursorOrder(t)
	now := time.Now().Truncate(time.Second)
	p := data.ObservedPayment{Network: mdb.NetworkSolana, Address: order.ReceiveAddress, Token: order.Token, AssetID: order.AssetID,
		TxHash: "historical-exception", EventID: "0", RawAmount: big.NewInt(3_000_000), Decimals: 6, BlockTimeMs: now.Add(-2 * time.Minute).UnixMilli()}
	if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"assigned_at_ms": now.Add(-3 * time.Minute).UnixMilli(), "payment_expires_at": now.Add(-time.Minute)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.ExpireExclusivePayments(); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(p.Network+"\x00"+p.TxHash+"\x00"+p.EventID)))
	receipt := &mdb.PaymentReceipt{EventKey: key, Network: p.Network, Address: p.Address, Token: p.Token, AssetID: p.AssetID,
		TxHash: p.TxHash, EventID: p.EventID, TradeID: order.TradeId, RawAmount: p.RawAmount.String(), Decimals: p.Decimals,
		BlockTimeMs: p.BlockTimeMs, Status: "exception", Reason: "closed_order"}
	if err := dao.Mdb.Create(receipt).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if handled, err := processObservedPayment(p); !handled || err != nil {
			t.Fatalf("receipt replay handled=%v err=%v", handled, err)
		}
	}
	got, _ := data.GetOrderInfoByTradeId(order.TradeId)
	var receipts []mdb.PaymentReceipt
	if err := dao.Mdb.Find(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	if got.Status != mdb.StatusPaySuccess || got.PaidAmount != 21 || len(receipts) != 1 || receipts[0].Status != "settled" || receipts[0].ID != receipt.ID {
		t.Fatalf("historical exception not recovered once: order=%+v receipts=%+v", got, receipts)
	}
}
