package task

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestTronPartialMainnetBlockUsesCompleteFallback(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	f, err := os.Open("../internal/troncodec/testdata/block-87034506.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	raw, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	var full Block
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatal(err)
	}
	if err := validateTronBlockResponse(raw, nil); err != nil {
		t.Fatalf("complete mainnet fixture invalid: %v", err)
	}
	counts := map[string]int{}
	for _, tx := range full.Transactions {
		if p := parseTRXTransfer(tx); p != nil && p.Status == "SUCCESS" {
			counts[p.To]++
		}
	}
	chosen := -1
	var transfer *TRXTransfer
	for i, tx := range full.Transactions {
		if p := parseTRXTransfer(tx); p != nil && p.Status == "SUCCESS" && p.RawSun >= 1_000_000 && counts[p.To] == 1 {
			chosen = i
			transfer = p
			break
		}
	}
	if chosen < 0 {
		t.Fatal("fixture has no unique TRX recipient")
	}
	var partial map[string]json.RawMessage
	if err := json.Unmarshal(raw, &partial); err != nil {
		t.Fatal(err)
	}
	var txs []json.RawMessage
	if err := json.Unmarshal(partial["transactions"], &txs); err != nil {
		t.Fatal(err)
	}
	partial["transactions"], err = json.Marshal(append(txs[:chosen], txs[chosen+1:]...))
	if err != nil {
		t.Fatal(err)
	}
	truncated, err := json.Marshal(partial)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTronBlockResponse(truncated, nil); err == nil {
		t.Fatal("partial mainnet block accepted")
	}
	expires := time.Now().Add(time.Hour)
	order := &mdb.Orders{TradeId: "partial-list", OrderId: "partial-list", Network: mdb.NetworkTron, Token: "TRX", Amount: 21, ActualAmount: 3, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
	if err := data.CreateExclusivePayment(order, mdb.ChainToken{Network: mdb.NetworkTron, Symbol: "TRX", Decimals: 6}, []mdb.WalletAddress{{Address: transfer.To}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(order).Update("assigned_at_ms", full.BlockHeader.RawData.Timestamp-60000).Error; err != nil {
		t.Fatal(err)
	}
	var backupCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(truncated) }))
	defer primary.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupCalls.Add(1); _, _ = w.Write(raw) }))
	defer backup.Close()
	for i, u := range []string{primary.URL, backup.URL} {
		purpose := mdb.RpcNodePurposeGeneral
		if i == 1 {
			purpose = mdb.RpcNodePurposePaidFallback
		}
		if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: u, Purpose: purpose, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
			t.Fatal(err)
		}
	}
	scanner := NewScanner()
	scanner.lastBlock = full.BlockHeader.RawData.Number - 1
	scanner.poll()
	stored, err := data.LoadPaymentCursor("tron:blocks")
	if err != nil {
		t.Fatal(err)
	}
	got, err := data.GetOrderInfoByTradeId(order.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || stored != strconv.FormatInt(full.BlockHeader.RawData.Number, 10) || backupCalls.Load() != 1 {
		t.Fatalf("order=%+v cursor=%s calls=%d err=%v", got, stored, backupCalls.Load(), err)
	}
	scanner.poll()
	var count int64
	dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
	if count != 1 {
		t.Fatalf("receipt count=%d", count)
	}
}
