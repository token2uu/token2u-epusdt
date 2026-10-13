package service

import (
	"encoding/json"
	"fmt"
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
)

type tronManualProof struct {
	raw    int64
	infoID string
}
type tronManualFixture struct {
	order *mdb.Orders
	token *mdb.ChainToken
	txID  string
	stamp int64
	proof atomic.Pointer[tronManualProof]
}

func setupTronManualFixture(t *testing.T, symbol string) *tronManualFixture {
	t.Helper()
	t.Cleanup(testutil.SetupTestDatabases(t))
	const receiveHex = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	receive, err := tronHexToAddress(receiveHex)
	if err != nil {
		t.Fatal(err)
	}
	f := &tronManualFixture{txID: strings.Repeat("a", 64), stamp: time.Now().Add(-10 * time.Second).UnixMilli()}
	f.order = createTimedAmountOrder(t, "tron-manual", mdb.NetworkTron, receive, 5, time.Minute)
	if symbol == "TRX" {
		if err := dao.Mdb.Create(&mdb.ChainToken{Network: mdb.NetworkTron, Symbol: symbol, Decimals: 6, Enabled: true}).Error; err != nil {
			t.Fatal(err)
		}
		if err := dao.Mdb.Model(f.order).Update("token", symbol).Error; err != nil {
			t.Fatal(err)
		}
		if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", f.order.TradeId).Update("token", symbol).Error; err != nil {
			t.Fatal(err)
		}
	}
	f.token, err = data.GetEnabledChainTokenBySymbol(mdb.NetworkTron, symbol)
	if err != nil {
		t.Fatal(err)
	}
	contractHex := ""
	if symbol != "TRX" {
		contractHex, err = tronAddressToHex(f.token.ContractAddress)
		if err != nil {
			t.Fatal(err)
		}
	}
	f.proof.Store(&tronManualProof{5000000, f.txID})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proof := f.proof.Load()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/wallet/gettransactionbyid":
			kind := "TriggerSmartContract"
			value := map[string]interface{}{}
			if symbol == "TRX" {
				kind = "TransferContract"
				value = map[string]interface{}{"to_address": receiveHex, "amount": proof.raw}
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"txID": f.txID, "ret": []interface{}{map[string]string{"contractRet": "SUCCESS"}}, "raw_data": map[string]interface{}{"contract": []interface{}{map[string]interface{}{"type": kind, "parameter": map[string]interface{}{"value": value}}}}})
		case "/wallet/gettransactioninfobyid":
			logs := []interface{}{}
			if symbol != "TRX" {
				logs = append(logs, map[string]interface{}{"address": strings.TrimPrefix(contractHex, "41"), "topics": []string{strings.TrimPrefix(erc20TransferEventHash.Hex(), "0x"), strings.Repeat("0", 64), strings.Repeat("0", 24) + strings.TrimPrefix(receiveHex, "41")}, "data": fmt.Sprintf("%064x", big.NewInt(proof.raw))})
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": proof.infoID, "blockNumber": 100, "blockTimeStamp": f.stamp, "receipt": map[string]string{"result": "SUCCESS"}, "log": logs})
		case "/wallet/getnowblock":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]int64{"number": 110}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposeManualVerify, Status: mdb.RpcNodeStatusOk, Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *tronManualFixture) scan() error {
	raw := f.proof.Load().raw
	if f.token.Symbol == "TRX" {
		return ProcessTronTRXTransfer(f.order.ReceiveAddress, raw, f.txID, f.stamp)
	}
	return ProcessTronTRC20Transfer(*f.token, f.order.ReceiveAddress, big.NewInt(raw), f.txID, f.stamp)
}
func TestManualTronInfoIdentityFailureCanRecover(t *testing.T) {
	for _, symbol := range []string{"USDT", "TRX"} {
		for _, badID := range []string{"", strings.Repeat("b", 64)} {
			t.Run(symbol+"/"+badID, func(t *testing.T) {
				f := setupTronManualFixture(t, symbol)
				f.proof.Store(&tronManualProof{5000000, badID})
				if _, err := SubmitCashierManualPayment(f.order.TradeId, f.txID); err == nil {
					t.Fatal("unbound info accepted")
				}
				got, err := data.GetOrderInfoByTradeId(f.order.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
					t.Fatalf("invalid info changed order: %+v %v", got, err)
				}
				f.proof.Store(&tronManualProof{5000000, "0X" + strings.ToUpper(f.txID)})
				if _, err := SubmitCashierManualPayment(f.order.TradeId, f.txID); err != nil {
					t.Fatal(err)
				}
				if err := data.ResetCallbackConfirmOk(f.order.TradeId); err != nil {
					t.Fatal(err)
				}
				if err := f.scan(); err != nil {
					t.Fatal(err)
				}
				got, err = data.GetOrderInfoByTradeId(f.order.TradeId)
				if err != nil || got.Status != mdb.StatusPaySuccess || got.CallBackConfirm != mdb.CallBackConfirmOk || got.BlockTransactionId != f.txID {
					t.Fatalf("recovery/replay: %+v %v", got, err)
				}
			})
		}
	}
}
func TestManualTronAmountFallbackPreservesOwnershipAndRestrictions(t *testing.T) {
	for _, symbol := range []string{"USDT", "TRX"} {
		for _, mode := range []string{"exact", "underpaid", "overpaid", "zero", "package", "generic", "exact-other-owner", "ambiguous-owner", "expired-timely", "reused-address"} {
			t.Run(symbol+"/"+mode, func(t *testing.T) {
				f := setupTronManualFixture(t, symbol)
				raw := int64(3000000)
				expected := float64(21)
				reject := false
				switch mode {
				case "exact":
					raw = 5000000
					expected = 0
				case "overpaid":
					raw = 7000000
					expected = 49
				case "zero":
					raw = 0
					reject = true
				case "package":
					reject = true
					if err := dao.Mdb.Model(f.order).Update("notify_url", "https://merchant.invalid/api/user/package/epay/notify").Error; err != nil {
						t.Fatal(err)
					}
				case "generic":
					reject = true
					if err := dao.Mdb.Model(f.order).Update("notify_url", "https://merchant.invalid/notify").Error; err != nil {
						t.Fatal(err)
					}
				case "expired-timely", "reused-address":
					assigned := time.Now().Add(-11 * time.Minute)
					deadline := time.Now().Add(-time.Minute)
					if err := dao.Mdb.Model(f.order).Updates(map[string]interface{}{
						"created_at": assigned, "assigned_at_ms": assigned.UnixMilli(), "payment_expires_at": deadline,
						"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedExpired,
					}).Error; err != nil {
						t.Fatal(err)
					}
					if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", f.order.TradeId).Update("expires_at", deadline).Error; err != nil {
						t.Fatal(err)
					}
					if mode == "expired-timely" {
						f.stamp = time.Now().Add(-2 * time.Minute).UnixMilli()
					} else {
						reject = true
						if err := data.UnLockTransactionByTradeId(f.order.TradeId); err != nil {
							t.Fatal(err)
						}
						current := createTimedAmountOrder(t, "current", mdb.NetworkTron, f.order.ReceiveAddress, 5, 30*time.Second)
						if symbol == "TRX" {
							if err := dao.Mdb.Model(current).Update("token", symbol).Error; err != nil {
								t.Fatal(err)
							}
							if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", current.TradeId).Update("token", symbol).Error; err != nil {
								t.Fatal(err)
							}
						}
					}
				case "exact-other-owner", "ambiguous-owner":
					reject = true
					amount := float64(3)
					if mode == "ambiguous-owner" {
						amount = 4
					}
					other := createTimedAmountOrder(t, "other", mdb.NetworkTron, f.order.ReceiveAddress, amount, time.Minute)
					if symbol == "TRX" {
						if err := dao.Mdb.Model(other).Update("token", symbol).Error; err != nil {
							t.Fatal(err)
						}
						if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", other.TradeId).Update("token", symbol).Error; err != nil {
							t.Fatal(err)
						}
					}
				}
				f.proof.Store(&tronManualProof{raw, f.txID})
				submit := SubmitCashierManualPayment
				if mode == "expired-timely" || mode == "reused-address" {
					submit = SubmitManualPayment
				}
				_, err := submit(f.order.TradeId, "0X"+strings.ToUpper(f.txID))
				got, loadErr := data.GetOrderInfoByTradeId(f.order.TradeId)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if reject {
					if err == nil || (got.Status != mdb.StatusWaitPay && !(mode == "reused-address" && got.Status == mdb.StatusExpired)) || got.BlockTransactionId != "" {
						t.Fatalf("invalid fallback accepted: %+v %v", got, err)
					}
					return
				}
				if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidAmount != expected || got.BlockTransactionId != f.txID {
					t.Fatalf("valid fallback failed: %+v %v", got, err)
				}
				if err := data.ResetCallbackConfirmOk(f.order.TradeId); err != nil {
					t.Fatal(err)
				}
				if err := f.scan(); err != nil {
					t.Fatal(err)
				}
				got, err = data.GetOrderInfoByTradeId(f.order.TradeId)
				if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk || got.PaidAmount != expected {
					t.Fatalf("replay changed settlement: %+v %v", got, err)
				}
			})
		}
	}
}
