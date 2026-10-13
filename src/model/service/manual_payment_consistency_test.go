package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

type manualReceiptRPC struct {
	receipt atomic.Pointer[types.Receipt]
	stamp   uint64
}

func (p *manualReceiptRPC) GetTransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	return p.receipt.Load(), nil
}
func (p *manualReceiptRPC) GetBlockByNumber(ctx context.Context, number string, full bool) (*types.Header, error) {
	return (&paymentIdentityRPC{stamp: p.stamp}).GetBlockByNumber(ctx, number, full)
}

func installManualReceiptRPC(t *testing.T, api interface{}) {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("eth", api); err != nil {
		t.Fatal(err)
	}
	h := httptest.NewServer(server)
	t.Cleanup(h.Close)
	t.Cleanup(server.Stop)
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkEthereum, Type: mdb.RpcNodeTypeHttp, Url: h.URL, Purpose: mdb.RpcNodePurposeManualVerify, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
}

func TestManualEvmInvalidEvidenceCanRecoverWithoutConsumingPayment(t *testing.T) {
	for _, kind := range []string{"orphan-block", "wrong-transaction", "wrong-log-transaction", "wrong-log-block", "removed-log", "missing-receipt", "short-data", "extra-topic"} {
		t.Run(kind, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			const owner = "0x1111111111111111111111111111111111111111"
			order := createTimedAmountOrder(t, "receipt-proof", mdb.NetworkEthereum, owner, 5, time.Minute)
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			hash := common.HexToHash("0x" + strings.Repeat("ec", 32))
			stamp := uint64(time.Now().Add(-10 * time.Second).Unix())
			entry := &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(owner).Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), TxHash: hash, BlockNumber: 100}
			valid := newPaymentIdentityReceipt(entry, stamp)
			bad := *valid
			badLog := *entry
			bad.Logs = []*types.Log{&badLog}
			otherHash := common.HexToHash("0x" + strings.Repeat("dd", 32))
			switch kind {
			case "orphan-block":
				bad.BlockHash = otherHash
				badLog.BlockHash = otherHash
			case "wrong-transaction":
				bad.TxHash = otherHash
				badLog.TxHash = otherHash
			case "wrong-log-transaction":
				badLog.TxHash = otherHash
			case "wrong-log-block":
				badLog.BlockNumber++
			case "removed-log":
				badLog.Removed = true
			case "short-data":
				badLog.Data = big.NewInt(5000000).Bytes()
			case "extra-topic":
				badLog.Topics = append(append([]common.Hash(nil), entry.Topics...), common.Hash{})
			}
			api := &manualReceiptRPC{stamp: stamp}
			api.receipt.Store(&bad)
			if kind == "missing-receipt" {
				api.receipt.Store(nil)
			}
			installManualReceiptRPC(t, api)
			if _, err := SubmitCashierManualPayment(order.TradeId, hash.Hex()); err == nil {
				t.Fatal("invalid evidence accepted")
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			var receipts int64
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil {
				t.Fatal(err)
			}
			if got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" || receipts != 0 {
				t.Fatalf("invalid attempt consumed payment: %+v receipts=%d", got, receipts)
			}
			api.receipt.Store(valid)
			if _, err := SubmitCashierManualPayment(order.TradeId, hash.Hex()); err != nil {
				t.Fatalf("recovered proof rejected: %v", err)
			}
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != mdb.StatusPaySuccess || got.BlockTransactionId != hash.Hex() {
				t.Fatalf("recovery did not settle: %+v", got)
			}
			if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
				t.Fatal(err)
			}
			if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), big.NewInt(5000000), hash.Hex(), int64(stamp)*1000, 0); err != nil {
				t.Fatal(err)
			}
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
				t.Fatalf("automatic replay requeued callback: %+v %v", got, err)
			}
		})
	}
}

func TestManualAndAutomaticSettlementPreserveAmountPrecision(t *testing.T) {
	for _, spec := range []struct {
		name string
		raw  int64
		paid float64
	}{
		{"exact", 5000000, 0}, {"rounded-down", 4996000, 0}, {"rounded-up", 5004000, 0},
		{"underpaid", 4990000, 34.93}, {"overpaid", 5010000, 35.07},
	} {
		for _, flow := range []string{"manual", "automatic"} {
			t.Run(spec.name+"/"+flow, func(t *testing.T) {
				t.Cleanup(testutil.SetupTestDatabases(t))
				const owner = "0x1111111111111111111111111111111111111111"
				order := createTimedAmountOrder(t, "precision-payment", mdb.NetworkEthereum, owner, 5, time.Minute)
				if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"amount_precision": 2, "payment_type": mdb.PaymentTypeEpay}).Error; err != nil {
					t.Fatal(err)
				}
				// Later configuration changes must not alter an existing order's quote.
				previous := data.GetSettingString(mdb.SettingKeyAmountPrecision, "")
				t.Cleanup(func() {
					if err := data.SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, previous, mdb.SettingTypeInt); err != nil {
						t.Error(err)
					}
				})
				if err := data.SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "6", mdb.SettingTypeInt); err != nil {
					t.Fatal(err)
				}
				asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
				if err != nil {
					t.Fatal(err)
				}
				hash := common.HexToHash("0x" + strings.Repeat("ea", 32))
				stamp := uint64(time.Now().Add(-10 * time.Second).Unix())
				raw := big.NewInt(spec.raw)
				entry := &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(owner).Bytes())}, Data: common.LeftPadBytes(raw.Bytes(), 32), TxHash: hash, BlockNumber: 100}
				installManualReceiptRPC(t, &paymentIdentityRPC{newPaymentIdentityReceipt(entry, stamp), stamp})
				scan := func() error {
					return ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), raw, hash.Hex(), int64(stamp)*1000, 0)
				}
				if flow == "manual" {
					_, err = SubmitCashierManualPayment(order.TradeId, hash.Hex())
				} else {
					err = scan()
				}
				if err != nil {
					t.Fatal(err)
				}
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				if got.Status != mdb.StatusPaySuccess || got.PaidAmount != spec.paid {
					t.Fatalf("inconsistent settlement: %+v expected paid_amount=%g", got, spec.paid)
				}
				if got.PaidCryptoAmount == "" {
					t.Fatal("raw crypto amount lost")
				}
				params, err := BuildEPayResultParams(got, &mdb.ApiKey{Pid: "1001", SecretKey: "test"})
				if err != nil {
					t.Fatal(err)
				}
				if params["money"] != "35.0000" || params["trade_status"] != "TRADE_SUCCESS" {
					t.Fatalf("wrong callback: %v", params)
				}
				if spec.paid == 0 && params["paid_money"] != "" {
					t.Fatalf("precision-matched payment changed full-payment callback: %v", params)
				}
				if spec.paid != 0 && params["paid_money"] == "" {
					t.Fatalf("partial/overpayment amount missing: %v", params)
				}
				if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
					t.Fatal(err)
				}
				if err := scan(); err != nil {
					t.Fatal(err)
				}
				got, err = data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk || got.PaidAmount != spec.paid {
					t.Fatalf("replay changed amount or callback: %+v %v", got, err)
				}
			})
		}
	}
}

func TestSolManualUsesCredentialsForTransactionAndConfirmation(t *testing.T) {
	for _, mode := range []string{"api-key", "url-key", "anonymous", "invalid-then-repaired-key"} {
		t.Run(mode, func(t *testing.T) {
			ledger := setupSolCursorLedger(t)
			isolateSolThrottle(t, 1000)
			isolateSolHistoryBackoff(t)
			if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", mdb.NetworkSolana).Update("min_confirmations", 12).Error; err != nil {
				t.Fatal(err)
			}
			order := createTimedAmountOrder(t, "auth-payment", mdb.NetworkSolana, solCursorTestOwner, 5, time.Minute)
			const signature = "authenticated-payment"
			ledger.add(signature, 2, "5000000")
			var confirmations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authorized := mode == "anonymous" || r.Header.Get("Authorization") == "Bearer private-test-key" || (mode == "url-key" && r.URL.Query().Get("key") == "private-test-key")
				if !authorized {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				var req struct{ Method string }
				if err = json.Unmarshal(body, &req); err != nil {
					t.Error(err)
					return
				}
				if req.Method == "getSignatureStatuses" {
					confirmations.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"value":[{"err":null,"confirmationStatus":"finalized","confirmations":null}]}}`))
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				ledger.ServeHTTP(w, r)
			}))
			defer server.Close()
			endpoint := server.URL
			key := ""
			switch mode {
			case "api-key":
				key = "private-test-key"
			case "invalid-then-repaired-key":
				key = "wrong-key"
			case "url-key":
				endpoint += "?key=private-test-key"
			}
			if err := dao.Mdb.Model(&mdb.RpcNode{}).Where("network = ?", mdb.NetworkSolana).Updates(map[string]interface{}{"url": endpoint, "api_key": key}).Error; err != nil {
				t.Fatal(err)
			}
			if mode == "invalid-then-repaired-key" {
				if _, err := SubmitCashierManualPayment(order.TradeId, signature); err == nil {
					t.Fatal("wrong credential accepted")
				}
				got, err := data.GetOrderInfoByTradeId(order.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
					t.Fatalf("authentication failure consumed payment: %+v %v", got, err)
				}
				if err := dao.Mdb.Model(&mdb.RpcNode{}).Where("network = ?", mdb.NetworkSolana).Update("api_key", "private-test-key").Error; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := SubmitCashierManualPayment(order.TradeId, signature); err != nil {
				t.Fatal(err)
			}
			if confirmations.Load() == 0 {
				t.Fatal("confirmation check was skipped")
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.Status != mdb.StatusPaySuccess {
				t.Fatalf("valid credentials did not settle: %+v %v", got, err)
			}
			if err := data.ResetCallbackConfirmOk(order.TradeId); err != nil {
				t.Fatal(err)
			}
			if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
				t.Fatal(err)
			}
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
				t.Fatalf("automatic replay requeued callback: %+v %v", got, err)
			}
		})
	}
}
