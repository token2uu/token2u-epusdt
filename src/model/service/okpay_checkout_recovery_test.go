package service

import (
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/http_client"
	"github.com/go-resty/resty/v2"
)

func reviewOkPaySetup(t *testing.T) *mdb.Orders {
	t.Helper()
	parent := exclusiveSelectionOrder(t)
	for key, value := range map[string]string{mdb.SettingKeyOkPayEnabled: "true", mdb.SettingKeyOkPayShopID: "review-shop", mdb.SettingKeyOkPayShopToken: "review-secret", mdb.SettingKeyOkPayCallbackURL: "https://93.184.216.34/okpay/notify"} {
		if err := data.SetSetting(mdb.SettingGroupOkPay, key, value, mdb.SettingTypeString); err != nil {
			t.Fatal(err)
		}
	}
	original := http_client.ClientFactory
	t.Cleanup(func() { http_client.ClientFactory = original })
	return parent
}

func reviewOkPayResponse() *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"status":"success","code":200,"data":{"order_id":"review-provider","pay_url":"https://pay.invalid/review"}}`))}
}

func TestFlowReviewOkPayRecoveryAndConcurrentRetry(t *testing.T) {
	for _, state := range []string{"failed", "creating", "old-closed-failed", "expired", "manually-closed"} {
		t.Run(state, func(t *testing.T) {
			parent := reviewOkPaySetup(t)
			var attempts []url.Values
			http_client.ClientFactory = func() *resty.Client {
				return resty.New().SetTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if err := r.ParseForm(); err != nil {
						return nil, err
					}
					attempts = append(attempts, r.PostForm)
					if len(attempts) == 1 {
						return nil, fmt.Errorf("lost response")
					}
					return reviewOkPayResponse(), nil
				}))
			}
			req := &request.SwitchNetworkRequest{TradeId: parent.TradeId, Network: "okpay", Token: "USDT"}
			if _, err := SwitchNetwork(req); err == nil {
				t.Fatal("expected first failure")
			}
			var child mdb.Orders
			if err := dao.Mdb.Where("parent_trade_id = ?", parent.TradeId).First(&child).Error; err != nil {
				t.Fatal(err)
			}
			switch state {
			case "creating":
				if err := dao.Mdb.Model(&mdb.ProviderOrder{}).Where("trade_id = ?", child.TradeId).Update("status", mdb.ProviderOrderStatusCreating).Error; err != nil {
					t.Fatal(err)
				}
			case "old-closed-failed":
				if err := dao.Mdb.Model(&child).Updates(map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedSuperseded, "is_selected": false}).Error; err != nil {
					t.Fatal(err)
				}
			case "expired":
				if err := dao.Mdb.Model(&child).Update("payment_expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
					t.Fatal(err)
				}
			case "manually-closed":
				if err := data.CloseExclusivePayment(parent.TradeId); err != nil {
					t.Fatal(err)
				}
			}
			// Retry must use persisted request fields after configuration changes.
			if err := data.SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeString); err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(&child).Update("redirect_url", "https://changed.invalid/return").Error; err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 4)
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, err := SwitchNetwork(req); errs <- err }()
			}
			wg.Wait()
			close(errs)
			blocked := state == "expired" || state == "manually-closed"
			for err := range errs {
				if (err != nil) != blocked {
					t.Fatalf("retry: %v blocked=%v", err, blocked)
				}
			}
			wantAttempts := 2
			if blocked {
				wantAttempts = 1
			}
			if len(attempts) != wantAttempts {
				t.Fatalf("requests=%d want=%d", len(attempts), wantAttempts)
			}
			if !blocked && !reflect.DeepEqual(attempts[0], attempts[1]) {
				t.Fatalf("retry changed request: %v -> %v", attempts[0], attempts[1])
			}
			if !blocked {
				selected, err := data.GetOrderInfoByTradeId(child.TradeId)
				if err != nil || !selected.IsSelected {
					t.Fatalf("recovered channel was not selected: %+v %v", selected, err)
				}
			}
			var count int64
			if err := dao.Mdb.Model(&mdb.Orders{}).Where("parent_trade_id = ?", parent.TradeId).Count(&count).Error; err != nil || count != 1 {
				t.Fatalf("children=%d err=%v", count, err)
			}
		})
	}
}

func TestFlowReviewOkPayCreateRacesWithSettlement(t *testing.T) {
	for _, outcome := range []string{"early-notify", "early-notify-lost-response", "parent-wins", "parent-closed"} {
		t.Run(outcome, func(t *testing.T) {
			parent := reviewOkPaySetup(t)
			var target string
			http_client.ClientFactory = func() *resty.Client {
				return resty.New().SetTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if err := r.ParseForm(); err != nil {
						return nil, err
					}
					target = r.PostForm.Get("unique_id")
					switch outcome {
					case "early-notify", "early-notify-lost-response":
						form := okPayNotifyTestForm("review-shop", "review-secret", "review-provider", target, r.PostForm.Get("amount"), "USDT")
						if err := HandleOkPayNotify(form, "early"); err != nil {
							return nil, err
						}
						if err := HandleOkPayNotify(form, "repeated"); err != nil {
							return nil, err
						}
					case "parent-wins":
						if err := dao.Mdb.Model(parent).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
							return nil, err
						}
						asset, err := data.GetEnabledChainTokenBySymbol(parent.Network, parent.Token)
						if err != nil {
							return nil, err
						}
						if err := ProcessTronTRC20Transfer(*asset, parent.ReceiveAddress, big.NewInt(5_000_000), "parent-winning-tx", time.Now().UnixMilli()); err != nil {
							return nil, err
						}
					case "parent-closed":
						if err := data.CloseExclusivePayment(parent.TradeId); err != nil {
							return nil, err
						}
					}
					if outcome == "early-notify-lost-response" {
						return nil, fmt.Errorf("response lost after payment")
					}
					return reviewOkPayResponse(), nil
				}))
			}
			view, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Network: "okpay", Token: "USDT"})
			if (err == nil) != (outcome == "early-notify") {
				t.Fatalf("view=%+v err=%v", view, err)
			}
			provider, loadErr := data.GetProviderOrderByTradeIDAndProvider(target, mdb.PaymentProviderOkPay)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			child, loadErr := data.GetOrderInfoByTradeId(target)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if strings.HasPrefix(outcome, "early-notify") {
				if child.Status != mdb.StatusPaySuccess || provider.Status != mdb.ProviderOrderStatusPaid {
					t.Fatalf("paid state downgraded: child=%+v provider=%+v", child, provider)
				}
				bad := okPayNotifyTestForm("review-shop", "review-secret", "different-provider", target, "5", "USDT")
				if err := HandleOkPayNotify(bad, "changed identity"); err == nil {
					t.Fatal("accepted different provider identity")
				}
			} else if child.Status != mdb.StatusExpired || provider.Status != mdb.ProviderOrderStatusExpired || provider.PayURL != "" {
				t.Fatalf("published losing checkout: child=%+v provider=%+v", child, provider)
			}
		})
	}
}
