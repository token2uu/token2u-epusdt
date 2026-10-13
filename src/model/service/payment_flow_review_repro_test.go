package service

// Regression acceptance tests for docs/payment-flow-review.md.

import (
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/http_client"
	"github.com/ethereum/go-ethereum/common"
	"github.com/go-resty/resty/v2"
)

func TestFlowReviewMultipleTransfersInOneTransaction(t *testing.T) {
	for _, mode := range []string{"legacy", "exclusive", "exclusive-first-mixed", "legacy-first-mixed"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			var orders []*mdb.Orders
			for i, address := range []string{"0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222"} {
				id := fmt.Sprintf("review-batch-%d", i)
				exclusive := mode == "exclusive" || (mode == "exclusive-first-mixed" && i == 0) || (mode == "legacy-first-mixed" && i == 1)
				if !exclusive {
					orders = append(orders, createTimedAmountOrder(t, id, mdb.NetworkEthereum, address, 5, time.Minute))
					continue
				}
				deadline := time.Now().Add(5 * time.Minute)
				o := &mdb.Orders{TradeId: id, OrderId: id, Network: mdb.NetworkEthereum, Token: "USDT", Amount: 5, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentExpiresAt: &deadline}
				if err := data.CreateExclusivePayment(o, *asset, []mdb.WalletAddress{{Address: address}}, false); err != nil {
					t.Fatal(err)
				}
				if err := dao.Mdb.Model(o).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
					t.Fatal(err)
				}
				orders = append(orders, o)
			}
			stamp := time.Now().Add(-5*time.Second).Unix() * 1000
			for replay := 0; replay < 2; replay++ {
				for i, o := range orders {
					if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, common.HexToAddress(asset.ContractAddress), common.HexToAddress(o.ReceiveAddress), big.NewInt(5_000_000), "0x"+strings.Repeat("ab", 32), stamp, uint(i)); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, o := range orders {
				got, err := data.GetOrderInfoByTradeId(o.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				if got.Status != mdb.StatusPaySuccess {
					var receipts []mdb.PaymentReceipt
					if err := dao.Mdb.Find(&receipts).Error; err != nil {
						t.Fatal(err)
					}
					t.Errorf("distinct transfer not credited: trade=%s status=%d receipts=%+v", got.TradeId, got.Status, receipts)
				}
			}
		})
	}
}

func TestFlowReviewChildReturnUsesMerchantOrder(t *testing.T) {
	parent := exclusiveSelectionOrder(t)
	if err := dao.Mdb.Model(parent).Updates(map[string]interface{}{"api_key_id": 2, "redirect_url": "https://merchant.invalid/return"}).Error; err != nil {
		t.Fatal(err)
	}
	childView, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Network: mdb.NetworkSolana, Token: "USDT"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := data.GetOrderInfoByTradeId(childView.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(child).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	_, err = processObservedPayment(data.ObservedPayment{Network: child.Network, Address: child.ReceiveAddress, Token: child.Token, AssetID: child.AssetID, RawAmount: big.NewInt(5_000_000), Decimals: 6, TxHash: "review-child-payment", EventID: "instruction:0", BlockTimeMs: time.Now().Add(-5*time.Second).Unix() * 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{parent.TradeId, child.TradeId} {
		redirect, err := ResolveEPayReturnRedirect(id)
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(redirect.TargetURL)
		if err != nil {
			t.Fatal(err)
		}
		if !redirect.IsMerchantRedirect || u.Query().Get("out_trade_no") != parent.OrderId || u.Query().Get("trade_no") != parent.TradeId {
			t.Errorf("return for %s uses wrong merchant identity: %s; want %s / %s", id, redirect.TargetURL, parent.OrderId, parent.TradeId)
		}
	}
}

func TestFlowReviewOkPayCreateRetry(t *testing.T) {
	parent := exclusiveSelectionOrder(t)
	for key, value := range map[string]string{mdb.SettingKeyOkPayEnabled: "true", mdb.SettingKeyOkPayShopID: "review-shop", mdb.SettingKeyOkPayShopToken: "review-secret", mdb.SettingKeyOkPayCallbackURL: "https://93.184.216.34/okpay/notify"} {
		if err := data.SetSetting(mdb.SettingGroupOkPay, key, value, mdb.SettingTypeString); err != nil {
			t.Fatal(err)
		}
	}
	original := http_client.ClientFactory
	t.Cleanup(func() { http_client.ClientFactory = original })
	attempts := 0
	http_client.ClientFactory = func() *resty.Client {
		return resty.New().SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, fmt.Errorf("temporary provider outage")
			}
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"status":"success","code":200,"data":{"order_id":"review-provider","pay_url":"https://pay.invalid/review"}}`))}, nil
		}))
	}
	req := &request.SwitchNetworkRequest{TradeId: parent.TradeId, Network: "okpay", Token: "USDT"}
	if _, err := SwitchNetwork(req); err == nil {
		t.Fatal("outage unexpectedly succeeded")
	}
	result, err := SwitchNetwork(req)
	if err != nil || result.PaymentUrl == "" {
		t.Fatalf("retry after outage cannot create checkout: attempts=%d result=%+v err=%v", attempts, result, err)
	}
}
