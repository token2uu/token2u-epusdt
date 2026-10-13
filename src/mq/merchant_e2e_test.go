//go:build merchant_e2e

package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/route"
	"github.com/GMWalletApp/epusdt/util/http_client"
	"github.com/go-resty/resty/v2"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"
)

func TestMerchantEndToEnd(t *testing.T) {
	binary := os.Getenv("EPUSDT_MERCHANT_TEST_BINARY")
	if binary == "" {
		t.Fatal("build merchant bridge with integration/new-api/run.sh first")
	}
	for _, mode := range []string{"parent-exact", "child-underpaid", "child-overpaid", "closed-then-success"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			viper.Set("epay_exclusive_addresses", true)
			if err := data.SetSetting("rate", "rate.forced_rate_list", `{"usd":{"usdt":1},"cny":{"usdt":1}}`, mdb.SettingTypeString); err != nil {
				t.Fatal(err)
			}
			for _, w := range []mdb.WalletAddress{{Network: mdb.NetworkTron, Address: "merchant-e2e-tron", Status: 1}, {Network: mdb.NetworkEthereum, Address: "0x1111111111111111111111111111111111111111", Status: 1}} {
				if err := dao.Mdb.Create(&w).Error; err != nil {
					t.Fatal(err)
				}
			}
			e := echo.New()
			route.RegisterRoute(e)
			gateway := httptest.NewServer(e)
			defer gateway.Close()
			viper.Set("app_uri", gateway.URL)
			ready := filepath.Join(t.TempDir(), "ready")
			cmd := exec.Command(binary, "-test.run=^TestEpusdtMerchantBridge$", "-test.v")
			cmd.Env = append(os.Environ(), "EPUSDT_E2E_GATEWAY="+gateway.URL, "EPUSDT_E2E_READY="+ready)
			var merchantLog bytes.Buffer
			cmd.Stdout = &merchantLog
			cmd.Stderr = &merchantLog
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			merchant := ""
			defer func() {
				if merchant != "" {
					r, err := http.Post(merchant+"/test/stop", "text/plain", nil)
					if err == nil {
						r.Body.Close()
					}
				} else {
					_ = cmd.Process.Kill()
				}
				if err := cmd.Wait(); err != nil {
					t.Errorf("merchant process: %v\n%s", err, merchantLog.String())
				}
				if t.Failed() {
					t.Log(merchantLog.String())
				}
			}()
			for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
				b, err := os.ReadFile(ready)
				if err == nil {
					merchant = string(b)
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if merchant == "" {
				t.Fatal("merchant did not start")
			}
			merchantURL, err := url.Parse(merchant)
			if err != nil {
				t.Fatal(err)
			}
			transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				if address != "93.184.216.34:80" {
					return nil, fmt.Errorf("unexpected outbound host %s", address)
				}
				return (&net.Dialer{}).DialContext(ctx, network, merchantURL.Host)
			}}
			defer transport.CloseIdleConnections()
			original := http_client.ClientFactory
			defer func() { http_client.ClientFactory = original }()
			http_client.ClientFactory = func() *resty.Client { return resty.New().SetTransport(transport) }
			r, err := http.Post(merchant+"/test/create", "application/json", strings.NewReader(`{"amount":5,"payment_method":"usdt.tron"}`))
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(r.Body)
			r.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			var created struct {
				Message string
				Data    struct {
					PayURL string `json:"pay_url"`
				}
			}
			if err := json.Unmarshal(body, &created); err != nil || created.Message != "success" {
				t.Fatalf("merchant create: %s %v", body, err)
			}
			var parent mdb.Orders
			if err := dao.Mdb.Where("parent_trade_id = ''").First(&parent).Error; err != nil {
				t.Fatal(err)
			}
			if parent.Currency != "USD" && parent.Currency != "usd" {
				t.Fatalf("fiat contract lost: %+v", parent)
			}
			target := &parent
			if strings.HasPrefix(mode, "child-") {
				view, err := service.SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Network: mdb.NetworkEthereum, Token: "USDT"})
				if err != nil {
					t.Fatal(err)
				}
				target, err = data.GetOrderInfoByTradeId(view.TradeId)
				if err != nil {
					t.Fatal(err)
				}
			}
			now := time.Now().Truncate(time.Second)
			if err := dao.Mdb.Model(target).Update("assigned_at_ms", now.Add(-2*time.Minute).UnixMilli()).Error; err != nil {
				t.Fatal(err)
			}
			if mode == "closed-then-success" {
				if err := dao.Mdb.Model(target).Update("payment_expires_at", now.Add(-30*time.Second)).Error; err != nil {
					t.Fatal(err)
				}
				processExpiredOrders()
				expired, err := data.GetOrderInfoByTradeId(parent.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				if expired.Status != mdb.StatusExpired {
					t.Fatalf("not expired: %+v", expired)
				}
				if err := sendOrderCallback(expired); err != nil {
					t.Fatal(err)
				}
			}
			amount := int64(5)
			if mode == "child-underpaid" || mode == "closed-then-success" {
				amount = 3
			}
			if mode == "child-overpaid" {
				amount = 7
			}
			asset, err := data.GetEnabledChainTokenBySymbol(target.Network, target.Token)
			if err != nil {
				t.Fatal(err)
			}
			p := data.ObservedPayment{Network: target.Network, Token: target.Token, Address: target.ReceiveAddress, AssetID: asset.ContractAddress, RawAmount: big.NewInt(amount * 1_000_000), Decimals: 6, TxHash: "e2e-" + mode, EventID: "event:0", BlockTimeMs: now.Add(-time.Minute).UnixMilli()}
			handled, err := data.ProcessExclusivePayment(p)
			if err != nil || !handled {
				t.Fatalf("settle handled=%v err=%v", handled, err)
			}
			paid, err := data.GetOrderInfoByTradeId(parent.TradeId)
			if err != nil || paid.Status != mdb.StatusPaySuccess {
				t.Fatalf("gateway unpaid: %+v %v", paid, err)
			}
			// Send once, then repeat concurrently as after an ACK loss/restart.
			if err := sendOrderCallback(paid); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 4)
			for i := 0; i < 4; i++ {
				go func() { done <- sendOrderCallback(paid) }()
			}
			for i := 0; i < 4; i++ {
				if err := <-done; err != nil {
					t.Error(err)
				}
			}
			// A delayed CLOSED must never undo successful merchant credit.
			closed := *paid
			closed.Status = mdb.StatusExpired
			if err := sendOrderCallback(&closed); err != nil {
				t.Fatal(err)
			}
			r, err = http.Get(merchant + "/test/state")
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				Quota        int
				QuotaPerUnit float64 `json:"quota_per_unit"`
				Orders       []struct {
					TradeNo       string `json:"trade_no"`
					Status        string
					Money         float64
					OriginalMoney float64 `json:"original_money"`
				}
			}
			err = json.NewDecoder(r.Body).Decode(&state)
			r.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			expected := 100000 + int(float64(amount)*state.QuotaPerUnit)
			if state.Quota != expected || len(state.Orders) != 1 || state.Orders[0].Status != "success" || state.Orders[0].Money != float64(amount) || state.Orders[0].OriginalMoney != 5 || state.Orders[0].TradeNo != parent.OrderId {
				t.Fatalf("merchant failed to converge: mode=%s state=%+v expectedQuota=%d", mode, state, expected)
			}
			redirect, err := service.ResolveEPayReturnRedirect(target.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(redirect.TargetURL)
			if err != nil {
				t.Fatal(err)
			}
			if u.Query().Get("out_trade_no") != parent.OrderId || u.Query().Get("trade_no") != parent.TradeId {
				t.Fatalf("wrong merchant return: %s", redirect.TargetURL)
			}
		})
	}
}
