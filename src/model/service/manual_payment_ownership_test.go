package service

import (
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCashierManualPaymentPreservesFundingAndOwnership(t *testing.T) {
	for _, scenario := range []string{"zero-auto", "zero-manual", "other-owner-auto", "other-owner-manual"} {
		t.Run(scenario, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			owner := "0x1111111111111111111111111111111111111111"
			target := createTimedAmountOrder(t, "target", mdb.NetworkEthereum, owner, 6, time.Minute)
			raw := big.NewInt(0)
			var original *mdb.Orders
			if strings.HasPrefix(scenario, "other-owner") {
				original = createTimedAmountOrder(t, "original", mdb.NetworkEthereum, owner, 5, time.Minute)
				raw.SetInt64(5000000)
			}
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			hash := common.HexToHash("0x" + strings.Repeat("ac", 32))
			stamp := uint64(time.Now().Add(-10 * time.Second).Unix())
			entry := &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(owner).Bytes())}, Data: common.LeftPadBytes(raw.Bytes(), 32), TxHash: hash, BlockNumber: 100, Index: 0}
			receipt := newPaymentIdentityReceipt(entry, stamp)
			server := rpc.NewServer()
			if err := server.RegisterName("eth", &paymentIdentityRPC{receipt, stamp}); err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(server)
			defer httpServer.Close()
			defer server.Stop()
			if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkEthereum, Type: mdb.RpcNodeTypeHttp, Url: httpServer.URL, Purpose: mdb.RpcNodePurposeManualVerify, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(scenario, "manual") {
				result, err := SubmitCashierManualPayment(target.TradeId, hash.Hex())
				t.Logf("cashier result=%+v err=%v", result, err)
			}
			if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), raw, hash.Hex(), int64(stamp)*1000, 0); err != nil {
				t.Fatal(err)
			}
			got, err := data.GetOrderInfoByTradeId(target.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("target status=%d amount=%g actual=%g paid=%g hash=%s callback=%d", got.Status, got.Amount, got.ActualAmount, got.PaidAmount, got.BlockTransactionId, got.CallBackConfirm)
			if got.Status == mdb.StatusPaySuccess {
				t.Errorf("unfunded/wrong-owner target marked paid")
			}
			if original != nil {
				orig, err := data.GetOrderInfoByTradeId(original.TradeId)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("original status=%d hash=%s", orig.Status, orig.BlockTransactionId)
				if orig.Status != mdb.StatusPaySuccess {
					t.Errorf("actual owner not settled")
				}
			}
		})
	}
}

func TestManualAmountFallbackRequiresUniqueHistoricalOwner(t *testing.T) {
	for _, scenario := range []string{"unique", "exact", "ambiguous", "expired-exact-owner", "expired-unique", "after-deadline", "before-assignment", "changed-address", "changed-address-exact", "below-minimum", "precision-exact", "expired-exact"} {
		t.Run(scenario, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			const owner = "0x1111111111111111111111111111111111111111"
			age := 3 * time.Minute
			if scenario == "after-deadline" || scenario == "expired-unique" || scenario == "expired-exact" {
				age = 11 * time.Minute
			}
			target := createTimedAmountOrder(t, "fallback", mdb.NetworkEthereum, owner, 6, age)
			raw := big.NewInt(5000000)
			stamp := uint64(time.Now().Add(-10 * time.Second).Unix())
			switch scenario {
			case "exact", "changed-address-exact":
				raw.SetInt64(6000000)
			case "precision-exact":
				raw.SetInt64(6000100)
				previousPrecision := data.GetSettingString(mdb.SettingKeyAmountPrecision, "")
				t.Cleanup(func() {
					// The settings cache outlives each temporary test database.
					if err := data.SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, previousPrecision, mdb.SettingTypeInt); err != nil {
						t.Error(err)
					}
				})
				if err := dao.Mdb.Model(target).Updates(map[string]interface{}{"amount_precision": 2, "notify_url": "https://merchant.invalid/notify"}).Error; err != nil {
					t.Fatal(err)
				}
				if err := data.SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "6", mdb.SettingTypeInt); err != nil {
					t.Fatal(err)
				}
			case "below-minimum":
				raw.SetInt64(1)
			case "ambiguous":
				createTimedAmountOrder(t, "competitor", mdb.NetworkEthereum, owner, 7, 3*time.Minute)
			case "before-assignment":
				if err := dao.Mdb.Model(target).Update("assigned_at_ms", time.Now().Add(-5*time.Second).UnixMilli()).Error; err != nil {
					t.Fatal(err)
				}
			case "expired-exact-owner":
				other := createTimedAmountOrder(t, "original", mdb.NetworkEthereum, owner, 5, 11*time.Minute)
				if err := dao.Mdb.Model(other).Updates(map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedExpired}).Error; err != nil {
					t.Fatal(err)
				}
				stamp = uint64(time.Now().Add(-2 * time.Minute).Unix())
			case "expired-unique", "expired-exact":
				if scenario == "expired-exact" {
					raw.SetInt64(6000000)
				}
				if err := dao.Mdb.Model(target).Updates(map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedExpired}).Error; err != nil {
					t.Fatal(err)
				}
				stamp = uint64(time.Now().Add(-2 * time.Minute).Unix())
			}
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "below-minimum" {
				if err := dao.Mdb.Model(asset).Update("min_amount", 1).Error; err != nil {
					t.Fatal(err)
				}
			}
			hash := common.HexToHash("0x" + strings.Repeat("da", 32))
			entry := &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(owner).Bytes())}, Data: common.LeftPadBytes(raw.Bytes(), 32), TxHash: hash, BlockNumber: 100}
			receipt := newPaymentIdentityReceipt(entry, stamp)
			server := rpc.NewServer()
			if err := server.RegisterName("eth", &paymentIdentityRPC{receipt, stamp}); err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(server)
			defer httpServer.Close()
			defer server.Stop()
			if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkEthereum, Type: mdb.RpcNodeTypeHttp, Url: httpServer.URL, Purpose: mdb.RpcNodePurposeManualVerify, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
				t.Fatal(err)
			}
			success := scenario == "unique" || scenario == "exact" || scenario == "expired-unique" || scenario == "expired-exact" || scenario == "precision-exact"
			if scenario == "changed-address" || scenario == "changed-address-exact" {
				verified, e := ValidateManualOrderPaymentWithPaidAmount(target, hash.Hex())
				if e != nil {
					t.Fatal(e)
				}
				newOwner := "0x2222222222222222222222222222222222222222"
				if e := dao.Mdb.Model(target).Update("receive_address", newOwner).Error; e != nil {
					t.Fatal(e)
				}
				err = OrderProcessing(&request.OrderProcessingRequest{TradeId: target.TradeId, Network: target.Network, Token: target.Token, ReceiveAddress: newOwner, Amount: target.ActualAmount, PaidAmount: verified.PaidAmount, ManualPayment: verified.Evidence, BlockTransactionId: verified.CanonicalTxID})
			} else if scenario == "expired-unique" || scenario == "expired-exact" {
				_, err = SubmitManualPayment(target.TradeId, hash.Hex())
			} else {
				_, err = SubmitCashierManualPayment(target.TradeId, strings.ToUpper(hash.Hex()))
			}
			if success && err != nil {
				t.Fatal(err)
			}
			if !success && err == nil {
				t.Fatal("unsafe fallback accepted")
			}
			got, e := data.GetOrderInfoByTradeId(target.TradeId)
			if e != nil {
				t.Fatal(e)
			}
			if !success {
				if got.Status == mdb.StatusPaySuccess || got.BlockTransactionId != "" || got.PaidCryptoAmount != "" {
					t.Fatalf("rejected fallback changed settlement: %+v", got)
				}
				if scenario == "expired-exact-owner" {
					if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), raw, hash.Hex(), int64(stamp)*1000, 0); err != nil {
						t.Fatal(err)
					}
					original, e := data.GetOrderInfoByTradeId("original")
					if e != nil || original.Status != mdb.StatusPaySuccess {
						t.Fatalf("original payment not recoverable: %+v %v", original, e)
					}
				}
				return
			}
			wantCrypto, wantPaid := "5", 29.17
			if scenario == "exact" || scenario == "expired-exact" {
				wantCrypto, wantPaid = "6", 0
			}
			if scenario == "precision-exact" {
				wantCrypto, wantPaid = "6.0001", 0
			}
			if got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != wantCrypto || got.PaidAmount != wantPaid || got.BlockTransactionId != hash.Hex() || got.CallBackConfirm != mdb.CallBackConfirmNo {
				t.Fatalf("wrong settlement: %+v", got)
			}
			if err := data.ResetCallbackConfirmOk(target.TradeId); err != nil {
				t.Fatal(err)
			}
			if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), raw, hash.Hex(), int64(stamp)*1000, 0); err != nil {
				t.Fatal(err)
			}
			got, e = data.GetOrderInfoByTradeId(target.TradeId)
			if e != nil || got.CallBackConfirm != mdb.CallBackConfirmOk || got.PaidCryptoAmount != wantCrypto {
				t.Fatalf("replay changed payment/callback: %+v %v", got, e)
			}
		})
	}
}
