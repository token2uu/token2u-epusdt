package mq

import (
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/response"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/http_client"
	"github.com/GMWalletApp/epusdt/util/sign"
	"github.com/go-resty/resty/v2"
)

type reviewTransport func(*http.Request) (*http.Response, error)

func (f reviewTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestFlowReviewGmpayChildCallbackEvidence(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	deadline := time.Now().Add(time.Minute)
	parent := &mdb.Orders{TradeId: "review-parent", OrderId: "review-merchant", Network: mdb.NetworkEthereum, Token: "USDT", ReceiveAddress: "parent-wallet", Amount: 35, ActualAmount: 10, PaymentType: mdb.PaymentTypeGmpay, PayProvider: mdb.PaymentProviderOnChain, Status: mdb.StatusWaitPay, ApiKeyID: 1, NotifyUrl: "https://merchant.invalid/notify", PaymentExpiresAt: &deadline, AssignedAtMs: time.Now().Add(-time.Minute).UnixMilli()}
	child := &mdb.Orders{TradeId: "review-child", OrderId: "review-child", ParentTradeId: parent.TradeId, Network: mdb.NetworkTron, Token: "USDT", ReceiveAddress: "child-wallet", Amount: 35, ActualAmount: 5, PaymentType: mdb.PaymentTypeGmpay, PayProvider: mdb.PaymentProviderOnChain, Status: mdb.StatusWaitPay, ApiKeyID: 1, PaymentExpiresAt: &deadline, AssignedAtMs: parent.AssignedAtMs}
	for _, order := range []*mdb.Orders{parent, child} {
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
	}
	asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkTron, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ProcessTronTRC20Transfer(*asset, child.ReceiveAddress, big.NewInt(5_000_000), "review-winning-tx", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	paid, err := data.GetOrderInfoByTradeId(parent.TradeId)
	if err != nil || paid.Status != mdb.StatusPaySuccess || paid.PayBySubId != child.ID {
		t.Fatalf("group settlement failed: %+v %v", paid, err)
	}
	original := http_client.ClientFactory
	t.Cleanup(func() { http_client.ClientFactory = original })
	var received response.OrderNotifyResponse
	http_client.ClientFactory = func() *resty.Client {
		return resty.New().SetTransport(reviewTransport(func(r *http.Request) (*http.Response, error) {
			if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok"))}, nil
		}))
	}
	if err := sendOrderCallback(paid); err != nil {
		t.Fatal(err)
	}
	if received.OrderId != parent.OrderId || received.TradeId != parent.TradeId {
		t.Fatal("merchant order identity changed")
	}
	if received.BlockTransactionId != "review-winning-tx" || received.ReceiveAddress != child.ReceiveAddress || received.ActualAmount != child.ActualAmount {
		t.Fatalf("callback contains losing channel evidence: %+v; paid child=%s address=%s amount=%v hash=review-winning-tx", received, child.TradeId, child.ReceiveAddress, child.ActualAmount)
	}
	first := received
	if err := sendOrderCallback(paid); err != nil {
		t.Fatal(err)
	}
	if received != first {
		t.Fatalf("retry changed payload: %+v -> %+v", first, received)
	}
	signature := received.Signature
	received.Signature = ""
	expected, err := sign.GetHMACSHA256(received, "test-token")
	if err != nil || signature != expected {
		t.Fatalf("wrong child-evidence signature: %s %s %v", signature, expected, err)
	}
	root, err := data.GetOrderInfoByTradeId(parent.TradeId)
	if err != nil || root.ReceiveAddress != parent.ReceiveAddress || root.ActualAmount != parent.ActualAmount || root.BlockTransactionId != "" {
		t.Fatalf("callback changed root history: %+v %v", root, err)
	}
}

func TestFlowReviewGmpayMissingWinningEvidenceDoesNotNotify(t *testing.T) {
	for _, broken := range []string{"missing", "wrong-parent", "unpaid", "empty-transaction"} {
		t.Run(broken, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			child := &mdb.Orders{TradeId: "broken-child", OrderId: "broken-child", ParentTradeId: "root", Status: mdb.StatusPaySuccess, BlockTransactionId: "paid-tx"}
			if broken == "wrong-parent" {
				child.ParentTradeId = "other-root"
			}
			if broken == "unpaid" {
				child.Status = mdb.StatusWaitPay
			}
			if broken == "empty-transaction" {
				child.BlockTransactionId = ""
			}
			if broken != "missing" {
				if err := dao.Mdb.Create(child).Error; err != nil {
					t.Fatal(err)
				}
			} else {
				child.ID = 999
			}
			root := &mdb.Orders{TradeId: "root", OrderId: "merchant", Status: mdb.StatusPaySuccess, PayBySubId: child.ID, ApiKeyID: 1, NotifyUrl: "https://merchant.invalid/notify", PaymentType: mdb.PaymentTypeGmpay}
			original := http_client.ClientFactory
			defer func() { http_client.ClientFactory = original }()
			called := false
			http_client.ClientFactory = func() *resty.Client { called = true; return resty.New() }
			if err := sendOrderCallback(root); err == nil || called {
				t.Fatalf("sent callback without valid evidence: err=%v called=%v", err, called)
			}
		})
	}
}
