//go:build merchant_e2e

package task_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"

	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/mq"
	"github.com/GMWalletApp/epusdt/route"
	"github.com/GMWalletApp/epusdt/task"
	"github.com/GMWalletApp/epusdt/util/http_client"
	appLog "github.com/GMWalletApp/epusdt/util/log"
	"github.com/go-resty/resty/v2"
	"github.com/labstack/echo/v4"
	"github.com/libtnb/sqlite"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func networkE2EMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type networkMerchantState struct {
	Quota        int
	QuotaPerUnit float64 `json:"quota_per_unit"`
	Orders       []struct {
		TradeNo       string `json:"trade_no"`
		Status        string
		Money         float64
		OriginalMoney float64 `json:"original_money"`
	}
}

type networkMerchantHarness struct {
	URL     string
	Gateway string
	DropACK atomic.Bool
}
type networkCallbackTransport struct {
	base    *http.Transport
	harness *networkMerchantHarness
}

func (r networkCallbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(req)
	if err == nil && r.harness.DropACK.CompareAndSwap(true, false) {
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, errors.New("injected ACK loss after merchant committed")
	}
	return response, err
}

func startNetworkMerchant(t *testing.T) *networkMerchantHarness {
	t.Helper()
	binary := os.Getenv("EPUSDT_MERCHANT_TEST_BINARY")
	if binary == "" {
		t.Fatal("use integration/new-api/run.sh with the explicit merchant path")
	}
	e := echo.New()
	route.RegisterRoute(e)
	gateway := httptest.NewServer(e)
	t.Cleanup(gateway.Close)
	viper.Set("app_uri", gateway.URL)
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(binary, "-test.run=^TestEpusdtMerchantBridge$")
	cmd.Env = append(os.Environ(), "EPUSDT_E2E_GATEWAY="+gateway.URL, "EPUSDT_E2E_READY="+ready)
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	networkE2EMust(t, cmd.Start())
	h := &networkMerchantHarness{Gateway: gateway.URL}
	t.Cleanup(func() {
		if h.URL == "" {
			_ = cmd.Process.Kill()
		} else {
			resp, err := http.Post(h.URL+"/test/stop", "text/plain", nil)
			if err == nil {
				resp.Body.Close()
			}
		}
		if err := cmd.Wait(); err != nil {
			t.Errorf("merchant: %v\n%s", err, logs.String())
		}
		if t.Failed() {
			t.Log(logs.String())
		}
	})
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		b, err := os.ReadFile(ready)
		if err == nil {
			h.URL = string(b)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if h.URL == "" {
		t.Fatal("merchant startup timed out")
	}
	u, err := url.Parse(h.URL)
	networkE2EMust(t, err)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "93.184.216.34:80" {
			return nil, fmt.Errorf("blocked unexpected callback destination: %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, u.Host)
	}}
	previous := http_client.ClientFactory
	http_client.ClientFactory = func() *resty.Client { return resty.New().SetTransport(networkCallbackTransport{transport, h}) }
	t.Cleanup(func() { http_client.ClientFactory = previous; transport.CloseIdleConnections() })
	return h
}
func (h *networkMerchantHarness) state(t *testing.T) networkMerchantState {
	t.Helper()
	resp, err := http.Get(h.URL + "/test/state")
	networkE2EMust(t, err)
	defer resp.Body.Close()
	var state networkMerchantState
	networkE2EMust(t, json.NewDecoder(resp.Body).Decode(&state))
	return state
}
func (h *networkMerchantHarness) create(t *testing.T, network string) *mdb.Orders {
	t.Helper()
	resp, err := http.Post(h.URL+"/test/create", "application/json", strings.NewReader(fmt.Sprintf(`{"amount":5,"payment_method":"usdt.%s"}`, network)))
	networkE2EMust(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	networkE2EMust(t, err)
	var result struct {
		Message string
		Data    struct {
			PayURL string `json:"pay_url"`
		}
	}
	networkE2EMust(t, json.Unmarshal(body, &result))
	if result.Message != "success" || result.Data.PayURL == "" {
		t.Fatalf("merchant create failed: %s", body)
	}
	var order mdb.Orders
	networkE2EMust(t, dao.Mdb.Where("parent_trade_id = ''").Order("id DESC").First(&order).Error)
	return &order
}
func assertNetworkMerchant(t *testing.T, h *networkMerchantHarness, order *mdb.Orders, amount float64, status string) {
	t.Helper()
	s := h.state(t)
	if len(s.Orders) != 1 || s.Quota != 100000+int(amount*s.QuotaPerUnit) || s.Orders[0].Status != status || s.Orders[0].TradeNo != order.OrderId {
		t.Fatalf("merchant state: %+v, want credit=%v status=%s", s, amount, status)
	}
	if amount > 0 && (s.Orders[0].Money != amount || s.Orders[0].OriginalMoney != 5) {
		t.Fatalf("merchant actual/original amount mismatch: %+v", s)
	}
}

func TestMerchantNetworkEndToEnd(t *testing.T) {
	for _, network := range []string{mdb.NetworkTron, mdb.NetworkAptos, mdb.NetworkSolana} {
		for _, scenario := range []string{"exact", "underpaid", "overpaid", "switch-child", "closed-before-deadline", "late-payment", "manual-close", "wrong-token", "wrong-recipient", "failed-chain-tx", "rpc-unavailable", "incomplete-rpc", "confirmation-delay", "credit-failure", "ack-loss", "restart-replay", "concurrent-replay", "settlement-db-failure", "address-reuse"} {
			t.Run(network+"/"+scenario, func(t *testing.T) { runMerchantNetworkScenario(t, network, scenario) })
		}
	}
}

func runMerchantNetworkScenario(t *testing.T, network, scenario string) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetNetworkE2ERPC()
	t.Cleanup(resetNetworkE2ERPC)
	viper.Set("epay_exclusive_addresses", true)
	networkE2EMust(t, data.SetSetting("rate", "rate.forced_rate_list", `{"usd":{"usdt":1},"cny":{"usdt":1}}`, mdb.SettingTypeString))
	owners := seedNetworkE2EAssets(t)
	h := startNetworkMerchant(t)
	initial := network
	if scenario == "switch-child" {
		initial = mdb.NetworkTron
		if network == mdb.NetworkTron {
			initial = mdb.NetworkSolana
		}
	}
	root := h.create(t, initial)
	target := root
	if scenario == "switch-child" {

		payload, _ := json.Marshal(map[string]string{"trade_id": root.TradeId, "network": network, "token": "USDT"})
		resp, err := http.Post(h.Gateway+"/pay/switch-network", "application/json", bytes.NewReader(payload))
		networkE2EMust(t, err)
		var result struct {
			Data struct {
				TradeId string `json:"trade_id"`
			}
		}
		networkE2EMust(t, json.NewDecoder(resp.Body).Decode(&result))
		resp.Body.Close()
		if result.Data.TradeId == "" {
			t.Fatal("HTTP switch returned no target")
		}
		target, err = data.GetOrderInfoByTradeId(result.Data.TradeId)
		networkE2EMust(t, err)
	}
	now := time.Now().Truncate(time.Second)
	networkE2EMust(t, dao.Mdb.Model(target).Update("assigned_at_ms", now.Add(-2*time.Minute).UnixMilli()).Error)
	amount := int64(5)
	if scenario == "underpaid" {
		amount = 3
	}
	if scenario == "overpaid" {
		amount = 7
	}
	closed := scenario == "closed-before-deadline" || scenario == "late-payment" || scenario == "manual-close"
	if closed {
		if scenario == "manual-close" {
			networkE2EMust(t, data.CloseExclusivePayment(target.TradeId))
		} else {
			networkE2EMust(t, dao.Mdb.Model(target).Update("payment_expires_at", now.Add(-30*time.Second)).Error)
			mq.E2EExpireOrders()
		}
		mq.E2ECallback(root.TradeId)
		status := "expired"
		if scenario == "manual-close" {
			status = "pending"
		}
		assertNetworkMerchant(t, h, root, 0, status)
	}
	blockTime := now.Add(-time.Minute)
	if scenario == "late-payment" {
		blockTime = now.Add(-10 * time.Second)
	}
	fixture := newNetworkE2ERPC(t, network, owners[network], amount, blockTime)
	if scenario == "confirmation-delay" {
		fixture.setFault("unconfirmed")
	}
	for _, fault := range []string{"wrong-token", "wrong-recipient", "failed-chain-tx", "rpc-unavailable", "incomplete-rpc"} {
		if scenario == fault {
			fixture.setFault(fault)
		}
	}
	networkE2EMust(t, fixture.resetCursor())
	if scenario == "settlement-db-failure" {
		networkE2EMust(t, dao.Mdb.Callback().Create().Before("gorm:create").Register("e2e_receipt_failure", func(tx *gorm.DB) {
			if tx.Statement.Table == "payment_receipt" {
				tx.AddError(errors.New("injected receipt persistence failure"))
			}
		}))
		t.Cleanup(func() { _ = dao.Mdb.Callback().Create().Remove("e2e_receipt_failure") })
	}
	retryFirst := scenario == "wrong-token" || scenario == "wrong-recipient" || scenario == "failed-chain-tx" || scenario == "rpc-unavailable" || scenario == "incomplete-rpc" || scenario == "confirmation-delay" || scenario == "settlement-db-failure"
	if retryFirst {
		_ = task.E2EScanNetwork(network, owners[network])
		fresh, err := data.GetOrderInfoByTradeId(root.TradeId)
		networkE2EMust(t, err)
		if fresh.Status != mdb.StatusWaitPay {
			t.Fatalf("unverified payment changed root: %+v", fresh)
		}
		var receipts int64
		networkE2EMust(t, dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("status = ?", "settled").Count(&receipts).Error)
		if receipts != 0 {
			t.Fatal("invalid payment settled")
		}
		assertNetworkMerchant(t, h, root, 0, "pending")
		if scenario == "rpc-unavailable" || scenario == "incomplete-rpc" || scenario == "settlement-db-failure" {
			fixture.assertInitialCursor(t)
		}
		if scenario == "settlement-db-failure" {
			networkE2EMust(t, dao.Mdb.Callback().Create().Remove("e2e_receipt_failure"))
		}
		fixture.recover(scenario == "wrong-token" || scenario == "wrong-recipient" || scenario == "failed-chain-tx")
		resetNetworkE2ERPC()
	}
	if scenario == "concurrent-replay" {
		var wg sync.WaitGroup
		errors := make(chan error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); errors <- task.E2EScanNetwork(network, owners[network]) }()
		}
		wg.Wait()
		close(errors)
		for err := range errors {
			networkE2EMust(t, err)
		}
	} else {
		networkE2EMust(t, task.E2EScanNetwork(network, owners[network]))
	}
	fixture.assertCompletedCursor(t)
	fresh, err := data.GetOrderInfoByTradeId(root.TradeId)
	networkE2EMust(t, err)
	if scenario == "late-payment" || scenario == "manual-close" {
		if fresh.Status != mdb.StatusExpired || fresh.PaidAmount != 0 {
			t.Fatalf("closed order credited: %+v", fresh)
		}
		var receipts []mdb.PaymentReceipt
		networkE2EMust(t, dao.Mdb.Find(&receipts).Error)
		if len(receipts) != 1 || receipts[0].Status != "exception" {
			t.Fatalf("missing review evidence: %+v", receipts)
		}
		status := "expired"
		if scenario == "manual-close" {
			status = "pending"
		}
		assertNetworkMerchant(t, h, root, 0, status)
		return
	}
	if fresh.Status != mdb.StatusPaySuccess || fresh.PaidAmount != float64(amount) {
		t.Fatalf("scanner did not settle root: %+v", fresh)
	}
	var receipts int64
	networkE2EMust(t, dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("status = ?", "settled").Count(&receipts).Error)
	if receipts != 1 {
		t.Fatalf("settled receipts=%d", receipts)
	}
	if scenario == "restart-replay" {
		networkE2EMust(t, fixture.resetCursor())
		runNetworkScannerSubprocess(t, network, owners[network])
		fixture.assertCompletedCursor(t)
	}
	if scenario == "credit-failure" {
		resp, err := http.Post(h.URL+"/test/fail-credit", "text/plain", nil)
		networkE2EMust(t, err)
		resp.Body.Close()
	}
	if scenario == "ack-loss" {
		h.DropACK.Store(true)
	}
	mq.E2ECallback(root.TradeId)
	if scenario == "credit-failure" || scenario == "ack-loss" {
		fresh, err = data.GetOrderInfoByTradeId(root.TradeId)
		networkE2EMust(t, err)
		if fresh.CallBackConfirm != mdb.CallBackConfirmNo {
			t.Fatal("failed/unacknowledged callback was confirmed")
		}
		if scenario == "credit-failure" {
			assertNetworkMerchant(t, h, root, 0, "pending")
		} else {
			assertNetworkMerchant(t, h, root, float64(amount), "success")
		}
		mq.E2ECallback(root.TradeId)
	}
	assertNetworkMerchant(t, h, root, float64(amount), "success")
	fresh, err = data.GetOrderInfoByTradeId(root.TradeId)
	networkE2EMust(t, err)
	if fresh.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatal("callback acknowledgement not persisted")
	}
	// Re-deliver the actual signed success payload concurrently, then delayed CLOSED.
	key, err := service.ResolveOrderApiKey(fresh)
	networkE2EMust(t, err)
	params, err := service.BuildEPayResultParams(fresh, key)
	networkE2EMust(t, err)
	callback := func(p map[string]string) error {
		q := url.Values{}
		for k, v := range p {
			q.Set(k, v)
		}
		resp, err := http.Get(h.URL + "/api/user/epay/notify?" + q.Encode())
		if err != nil {
			return err
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(b)) != "success" {
			return fmt.Errorf("merchant rejected replay: %s", b)
		}
		return nil
	}
	results := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() { results <- callback(params) }()
	}
	for i := 0; i < 4; i++ {
		networkE2EMust(t, <-results)
	}
	late := *fresh
	late.Status = mdb.StatusExpired
	params, err = service.BuildEPayResultParams(&late, key)
	networkE2EMust(t, err)
	networkE2EMust(t, callback(params))
	networkE2EMust(t, task.E2EScanNetwork(network, owners[network]))
	networkE2EMust(t, dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("status = ?", "settled").Count(&receipts).Error)
	if receipts != 1 {
		t.Fatalf("replay receipts=%d", receipts)
	}
	assertNetworkMerchant(t, h, root, float64(amount), "success")
	redirect, err := service.ResolveEPayReturnRedirect(target.TradeId)
	networkE2EMust(t, err)
	u, err := url.Parse(redirect.TargetURL)
	networkE2EMust(t, err)
	if u.Query().Get("out_trade_no") != root.OrderId || u.Query().Get("trade_no") != root.TradeId {
		t.Fatalf("lost root identity: %s", redirect.TargetURL)
	}
	if scenario == "address-reuse" {
		next := h.create(t, network)
		if next.ReceiveAddress != root.ReceiveAddress || next.TradeId == root.TradeId {
			t.Fatal("address was not reassigned to a distinct order")
		}
		networkE2EMust(t, dao.Mdb.Model(next).Update("assigned_at_ms", now.Add(-10*time.Second).UnixMilli()).Error)
		// Re-fetch the old chain transfer from a fresh process while the new
		// order holds the address. Old money must never pay the new order.
		networkE2EMust(t, fixture.resetCursor())
		runNetworkScannerSubprocess(t, network, owners[network])
		fixture.assertCompletedCursor(t)
		stillPending, err := data.GetOrderInfoByTradeId(next.TradeId)
		networkE2EMust(t, err)
		if stillPending.Status != mdb.StatusWaitPay {
			t.Fatalf("old transfer paid new order: %+v", stillPending)
		}
		state := h.state(t)
		if len(state.Orders) != 2 || state.Orders[1].Status != "pending" || state.Quota != 100000+int(5*state.QuotaPerUnit) {
			t.Fatalf("reassigned order changed merchant balance: %+v", state)
		}
		fixture.mu.Lock()
		fixture.stamp = now.Add(-5 * time.Second)
		fixture.mu.Unlock()
		fixture.recover(true)
		networkE2EMust(t, task.E2EScanNetwork(network, owners[network]))
		fixture.assertCompletedCursor(t)
		paidNext, err := data.GetOrderInfoByTradeId(next.TradeId)
		networkE2EMust(t, err)
		if paidNext.Status != mdb.StatusPaySuccess {
			t.Fatalf("new transfer failed: %+v", paidNext)
		}
		mq.E2ECallback(next.TradeId)
		state = h.state(t)
		if len(state.Orders) != 2 || state.Orders[0].Status != "success" || state.Orders[1].Status != "success" || state.Quota != 100000+int(10*state.QuotaPerUnit) {
			t.Fatalf("two payments failed to converge: %+v", state)
		}
		networkE2EMust(t, dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("status = ?", "settled").Count(&receipts).Error)
		if receipts != 2 {
			t.Fatalf("two orders require exactly two settled receipts: %d", receipts)
		}
	}
}

func resetNetworkE2ERPC() {
	data.ResetRpcEndpointPolicyForTest()
	data.ResetRpcFailoverForTest()
	data.ResetRpcRuntimeStatsForTest()
}
func runNetworkScannerSubprocess(t *testing.T, network, owner string) {
	t.Helper()
	var mainDB, runtimeDB struct{ File string }
	networkE2EMust(t, dao.Mdb.Raw("PRAGMA database_list").Scan(&mainDB).Error)
	networkE2EMust(t, dao.RuntimeDB.Raw("PRAGMA database_list").Scan(&runtimeDB).Error)
	cmd := exec.Command(os.Args[0], "-test.run=^TestMerchantNetworkScannerProcess$")
	cmd.Env = append(os.Environ(), "EPUSDT_SCANNER_MAIN="+mainDB.File, "EPUSDT_SCANNER_RUNTIME="+runtimeDB.File, "EPUSDT_SCANNER_NETWORK="+network, "EPUSDT_SCANNER_OWNER="+owner)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scanner fresh process: %v\n%s", err, output)
	}
}
func TestMerchantNetworkScannerProcess(t *testing.T) {
	path := os.Getenv("EPUSDT_SCANNER_MAIN")
	if path == "" {
		t.Skip("subprocess only")
	}
	viper.Set("rpc_public_requests_per_second", 1000)
	viper.Set("order_expiration_time", 10)
	appLog.Sugar = zap.NewNop().Sugar()
	open := func(path string) *gorm.DB {
		db, err := gorm.Open(sqlite.Open(path), &gorm.Config{NamingStrategy: schema.NamingStrategy{SingularTable: true}, Logger: logger.Default.LogMode(logger.Silent)})
		networkE2EMust(t, err)
		sql, err := db.DB()
		networkE2EMust(t, err)
		sql.SetMaxOpenConns(1)
		t.Cleanup(func() { sql.Close() })
		return db
	}
	dao.Mdb = open(path)
	dao.RuntimeDB = open(os.Getenv("EPUSDT_SCANNER_RUNTIME"))
	networkE2EMust(t, task.E2EScanNetwork(os.Getenv("EPUSDT_SCANNER_NETWORK"), os.Getenv("EPUSDT_SCANNER_OWNER")))
}
