package service

import (
	"encoding/json"
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExactManualPaymentCannotClaimReusedAddress(t *testing.T) {
	for _, mode := range []string{"automatic-control", "cashier-first", "admin-expired-first"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			owner := "0x1111111111111111111111111111111111111111"
			old := createTimedAmountOrder(t, "old", mdb.NetworkEthereum, owner, 5, 11*time.Minute)
			if err := dao.Mdb.Model(old).Update("notify_url", "https://merchant.invalid/notify").Error; err != nil {
				t.Fatal(err)
			}
			if mode == "admin-expired-first" {
				if _, err := data.UpdateOrderIsExpirationById(old.ID, time.Now().Add(-10*time.Minute), true); err != nil {
					t.Fatal(err)
				}
			}
			current := createTimedAmountOrder(t, "current", mdb.NetworkEthereum, owner, 5, 30*time.Second)
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkEthereum, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			hash := common.HexToHash("0x" + strings.Repeat("cd", 32))
			stamp := uint64(time.Now().Add(-10 * time.Second).Unix())
			entry := &types.Log{Address: common.HexToAddress(asset.ContractAddress), Topics: []common.Hash{erc20TransferEventHash, {}, common.BytesToHash(common.HexToAddress(owner).Bytes())}, Data: common.LeftPadBytes(big.NewInt(5000000).Bytes(), 32), TxHash: hash, BlockNumber: 100}
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
			if mode == "cashier-first" {
				r, e := SubmitCashierManualPayment(old.TradeId, hash.Hex())
				t.Logf("cashier=%+v err=%v", r, e)
			}
			if mode == "admin-expired-first" {
				r, e := SubmitManualPayment(old.TradeId, hash.Hex())
				t.Logf("admin=%+v err=%v", r, e)
			}
			if err := ProcessConfirmedEvmERC20Transfer(mdb.NetworkEthereum, entry.Address, common.HexToAddress(owner), big.NewInt(5000000), hash.Hex(), int64(stamp)*1000, 0); err != nil {
				t.Fatal(err)
			}
			old, err = data.GetOrderInfoByTradeId(old.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			current, err = data.GetOrderInfoByTradeId(current.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("old status=%d hash=%s; current status=%d hash=%s", old.Status, old.BlockTransactionId, current.Status, current.BlockTransactionId)
			if old.Status == mdb.StatusPaySuccess || current.Status != mdb.StatusPaySuccess {
				t.Fatal("exact-match manual path stole payment from current owner")
			}
		})
	}
}

func TestSolanaManualAcceptsConfirmedInnerTransfer(t *testing.T) {
	for _, mode := range []string{"top-level-control", "inner"} {
		t.Run(mode, func(t *testing.T) {
			ledger := setupSolCursorLedger(t)
			isolateSolThrottle(t, 1000)
			isolateSolHistoryBackoff(t)
			if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", mdb.NetworkSolana).Update("min_confirmations", 1).Error; err != nil {
				t.Fatal(err)
			}
			order := createTimedAmountOrder(t, "manual-cpi", mdb.NetworkSolana, solCursorTestOwner, 5, time.Minute)
			sig := "confirmed-payment"
			ledger.add(sig, 2, "5000000")
			if mode == "inner" {
				detail := ledger.transactions[sig].(map[string]interface{})
				message := detail["transaction"].(map[string]interface{})["message"].(map[string]interface{})
				detail["meta"].(map[string]interface{})["innerInstructions"] = []interface{}{map[string]interface{}{"index": 0, "instructions": message["instructions"]}}
				message["instructions"] = []interface{}{map[string]interface{}{"programId": "custom-program", "data": "abc", "accounts": []interface{}{}}}
			}
			result, err := SubmitCashierManualPayment(order.TradeId, sig)
			t.Logf("manual=%+v err=%v", result, err)
			if err != nil {
				t.Errorf("confirmed payment cannot be manually reconciled: %v", err)
			}
			if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
				t.Fatal(err)
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("after automatic scan status=%d hash=%s", got.Status, got.BlockTransactionId)
			if got.Status != mdb.StatusPaySuccess {
				t.Fatal("fixture not a valid incoming payment")
			}
		})
	}
}

func TestSolanaManualRejectsIncompleteEvidenceAndAllowsRetry(t *testing.T) {
	for _, kind := range []string{"wrong-signature", "missing-status", "wrong-decimals", "incomplete-inner"} {
		t.Run(kind, func(t *testing.T) {
			ledger := setupSolCursorLedger(t)
			isolateSolThrottle(t, 1000)
			isolateSolHistoryBackoff(t)
			if err := dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", mdb.NetworkSolana).Update("min_confirmations", 1).Error; err != nil {
				t.Fatal(err)
			}
			order := createTimedAmountOrder(t, "retry-proof", mdb.NetworkSolana, solCursorTestOwner, 5, time.Minute)
			sig := "verified-transaction"
			ledger.add(sig, 2, "5000000")
			valid := ledger.transactions[sig]
			encoded, err := json.Marshal(valid)
			if err != nil {
				t.Fatal(err)
			}
			var bad map[string]interface{}
			if err := json.Unmarshal(encoded, &bad); err != nil {
				t.Fatal(err)
			}
			meta := bad["meta"].(map[string]interface{})
			transaction := bad["transaction"].(map[string]interface{})
			switch kind {
			case "wrong-signature":
				transaction["signatures"] = []string{"other-transaction"}
			case "missing-status":
				delete(meta, "err")
			case "wrong-decimals":
				transaction["message"].(map[string]interface{})["instructions"].([]interface{})[0].(map[string]interface{})["parsed"].(map[string]interface{})["info"].(map[string]interface{})["tokenAmount"].(map[string]interface{})["decimals"] = 9
			case "incomplete-inner":
				meta["innerInstructions"] = []interface{}{map[string]interface{}{"index": 0}}
			}
			ledger.transactions[sig] = bad
			if _, err := SubmitCashierManualPayment(order.TradeId, sig); err == nil {
				t.Fatal("incomplete evidence accepted")
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
				t.Fatalf("failed attempt consumed order: %+v %v", got, err)
			}
			ledger.transactions[sig] = valid
			if _, err := SubmitCashierManualPayment(order.TradeId, sig); err != nil {
				t.Fatalf("corrected evidence not retryable: %v", err)
			}
		})
	}
}
