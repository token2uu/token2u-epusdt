package service

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/dromara/carbon/v2"
	"github.com/ethereum/go-ethereum/common"
)

func TestReviewLegacyHistoryMustNotBlockNewExclusiveAssignment(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	expiry := now.Add(9 * time.Minute)
	old := &mdb.Orders{TradeId: "old-paid", OrderId: "old-paid", Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Status: mdb.StatusPaySuccess, ActualAmount: 5, Amount: 35, BlockTransactionId: "old-payment"}
	old.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-time.Minute)))
	if err := dao.Mdb.Create(old).Error; err != nil {
		t.Fatal(err)
	}
	current := &mdb.Orders{TradeId: "new-exclusive", OrderId: "new-exclusive", Network: "tron", Token: "USDT", Status: mdb.StatusWaitPay, ActualAmount: 5, Amount: 35, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expiry}
	asset := mdb.ChainToken{ContractAddress: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", Decimals: 6}
	if err := data.CreateExclusivePayment(current, asset, []mdb.WalletAddress{{Address: "wallet"}}, false); err != nil {
		t.Fatal(err)
	}
	p := data.ObservedPayment{Network: "tron", Token: "USDT", Address: "wallet", AssetID: asset.ContractAddress, RawAmount: big.NewInt(5_000_000), Decimals: 6, TxHash: "new-payment", EventID: "0", BlockTimeMs: time.Now().UnixMilli()}
	if _, err := processObservedPayment(p); err != nil {
		t.Fatal(err)
	}
	got, _ := data.GetOrderInfoByTradeId(current.TradeId)
	if got.Status != mdb.StatusPaySuccess {
		t.Fatalf("new exclusive payment blocked by completed legacy history: status=%d", got.Status)
	}
}

func TestReviewPaidSolanaExpiryDuringRPCMustNotConsumePayment(t *testing.T) {
	l := setupSolCursorLedger(t)
	isolateSolThrottle(t, 1000)
	isolateSolHistoryBackoff(t)
	data.ResetRpcEndpointPolicyForTest()
	t.Cleanup(data.ResetRpcEndpointPolicyForTest)
	expiry := time.Now().Add(time.Second)
	o := &mdb.Orders{TradeId: "legacy-race", OrderId: "legacy-race", Network: mdb.NetworkSolana, Token: "USDT", ReceiveAddress: solCursorTestOwner, Status: mdb.StatusWaitPay, ActualAmount: 5, Amount: 35}
	o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(expiry.Add(-10 * time.Minute)))
	if err := dao.Mdb.Create(o).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.LockTransaction(o.Network, o.ReceiveAddress, o.Token, o.TradeId, 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	l.add("on-time-payment", 2, "5000000")
	paidAt := time.Now().Add(-10 * time.Second).Unix()
	l.signatures[0].BlockTime = &paidAt
	l.transactions["on-time-payment"].(map[string]interface{})["blockTime"] = paidAt
	var expireOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expireOnce.Do(func() {
			time.Sleep(time.Until(expiry) + 20*time.Millisecond)
			if expired, err := data.UpdateOrderIsExpirationById(o.ID, time.Now().Add(-10*time.Minute), false); err != nil || !expired {
				t.Errorf("fixture expiry failed: %v %v", expired, err)
			}
			if err := data.UnLockTransactionByTradeId(o.TradeId); err != nil {
				t.Error(err)
			}
		})
		l.ServeHTTP(w, r)
	}))
	defer server.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusOk}).Error; err != nil {
		t.Fatal(err)
	}
	ctx := WithPaidPaymentReview(context.Background())
	for i := 0; i < 2; i++ {
		if err := ScanSolanaAddressWithContext(ctx, solCursorTestOwner); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := data.GetOrderInfoByTradeId(o.TradeId)
	cursor, _ := data.LoadPaymentCursor("solana:payment_review:" + l.ata)
	if got.Status != mdb.StatusPaySuccess || got.CallBackConfirm != mdb.CallBackConfirmNo || cursor != "on-time-payment" {
		t.Fatalf("expiry race permanently consumed on-time evidence: status=%d paid_cursor=%s", got.Status, cursor)
	}
}

func TestReviewAptosPendingPastDeadlineSettlesBeforeCachingTransfer(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	deadline := now.Add(-time.Second)
	o := &mdb.Orders{TradeId: "aptos-expiry-race", OrderId: "aptos-expiry-race", Network: mdb.NetworkAptos, Token: "USDT", ReceiveAddress: "0xa", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentExpiresAt: &deadline}
	o.ReceiveAddress, _ = addressutil.NormalizeMoveAddress(o.ReceiveAddress)
	o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-time.Minute)))
	if err := dao.Mdb.Create(o).Error; err != nil {
		t.Fatal(err)
	}
	if err := data.LockTransaction(o.Network, o.ReceiveAddress, o.Token, o.TradeId, 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	transfer := MoveObservedTransfer{Network: o.Network, ReceiveAddress: o.ReceiveAddress, Token: o.Token, RawAmount: big.NewInt(5_000_000), Decimals: 6, Amount: 5, TxID: "aptos-payment", TransferKey: "aptos-race", BlockTimeMs: now.Add(-10 * time.Second).UnixMilli()}
	for i := 0; i < 2; i++ {
		if err := ProcessMoveObservedTransferResult(transfer); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := data.GetOrderInfoByTradeId(o.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.BlockTransactionId != transfer.TxID {
		t.Fatalf("transfer consumed during expiry: %+v", got)
	}
}

func TestReviewEvmLegacyStreamNeverReopensManualOrUnknownClosure(t *testing.T) {
	for _, reason := range []string{mdb.OrderClosedManual, mdb.OrderClosedSuperseded, ""} {
		t.Run(fmt.Sprintf("reason-%s", reason), func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			contract := common.HexToAddress("0x5555555555555555555555555555555555555555")
			owner := common.HexToAddress("0x6666666666666666666666666666666666666666")
			upsertTaskServiceTestChainToken(t, mdb.ChainToken{Network: mdb.NetworkBsc, Symbol: "USDT", ContractAddress: contract.Hex(), Decimals: 6, Enabled: true})
			now := time.Now()
			deadline := now.Add(-time.Minute)
			o := &mdb.Orders{TradeId: "closed", OrderId: "closed", Network: mdb.NetworkBsc, Token: "USDT", ReceiveAddress: strings.ToLower(owner.Hex()), Status: mdb.StatusExpired, ClosedReason: reason, PaymentExpiresAt: &deadline, Amount: 35, ActualAmount: 5}
			o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-3 * time.Minute)))
			if err := dao.Mdb.Create(o).Error; err != nil {
				t.Fatal(err)
			}
			TryProcessEvmERC20Transfer(o.Network, contract, owner, big.NewInt(5_000_000), "closed-payment", now.Add(-2*time.Minute).UnixMilli())
			got, _ := data.GetOrderInfoByTradeId(o.TradeId)
			if got.Status != mdb.StatusExpired || got.BlockTransactionId != "" {
				t.Fatalf("legacy stream reopened closed order: %+v", got)
			}
		})
	}
}

func TestReviewLateLegacyPaymentNeverUsesStillActiveLock(t *testing.T) {
	for _, network := range []string{mdb.NetworkTron, mdb.NetworkAptos} {
		t.Run(network, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			now := time.Now()
			deadline := now.Add(-time.Second)
			owner := "wallet"
			if network == mdb.NetworkAptos {
				owner, _ = addressutil.NormalizeMoveAddress("0xa")
			}
			o := &mdb.Orders{TradeId: "late", OrderId: "late", Network: network, Token: "USDT", ReceiveAddress: owner, Status: mdb.StatusWaitPay, PaymentExpiresAt: &deadline, Amount: 35, ActualAmount: 5}
			o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-time.Minute)))
			if err := dao.Mdb.Create(o).Error; err != nil {
				t.Fatal(err)
			}
			if err := data.LockTransaction(o.Network, owner, o.Token, o.TradeId, 5, time.Minute); err != nil {
				t.Fatal(err)
			}
			if network == mdb.NetworkTron {
				if err := ProcessTronTRC20Transfer(mdb.ChainToken{Symbol: "USDT", Decimals: 6}, owner, big.NewInt(5_000_000), "late-payment", now.UnixMilli()); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Cleanup(func() { gProcessedMoveTx.Delete("late-aptos") })
				if err := ProcessMoveObservedTransferResult(MoveObservedTransfer{Network: network, ReceiveAddress: owner, Token: "USDT", Amount: 5, RawAmount: big.NewInt(5_000_000), Decimals: 6, TxID: "late-payment", TransferKey: "late-aptos", BlockTimeMs: now.UnixMilli()}); err != nil {
					t.Fatal(err)
				}
			}
			got, _ := data.GetOrderInfoByTradeId(o.TradeId)
			if got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" {
				t.Fatalf("late payment used active lock: %+v", got)
			}
		})
	}
}

func TestReviewTonExclusiveHistoryPreventsLegacyAddressOverlap(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(fmt.Sprintf("retired-%v", retired), func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			parsed, err := addressutil.ParseTonMainnetAddress("EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := data.AddWalletAddressWithNetwork(mdb.NetworkTon, parsed.Display); err != nil {
				t.Fatal(err)
			}
			if err := data.SetSetting("rate", "rate.forced_rate_list", `{"cny":{"ton":0.5}}`, "json"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Minute)
			o := &mdb.Orders{TradeId: "exclusive", OrderId: "exclusive", Network: mdb.NetworkTon, Token: "TON", Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentExpiresAt: &deadline}
			if err := data.CreateExclusivePayment(o, mdb.ChainToken{Decimals: 9}, []mdb.WalletAddress{{Address: parsed.Display}}, false); err != nil {
				t.Fatal(err)
			}
			if retired {
				if err := data.CloseExclusivePayment(o.TradeId); err != nil {
					t.Fatal(err)
				}
			}
			req := newCreateTransactionRequest("legacy-overlap", 10)
			req.Network = mdb.NetworkTon
			req.Token = "TON"
			if _, err := CreateTransaction(req, nil); err != constant.NotAvailableAmountErr {
				t.Fatalf("exclusive address must leave no available legacy amount: %v", err)
			}
			legacy, err := data.GetOrderInfoByOrderId(req.OrderId)
			if err != nil || legacy.ID != 0 {
				t.Fatalf("legacy order created on exclusive address: order=%+v err=%v", legacy, err)
			}
		})
	}
}

func TestReviewLegacyGroupSettlementRollsBackOnParentFailure(t *testing.T) {
	for _, failure := range []string{"expired", "missing", "write-failure"} {
		t.Run(failure, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			root := &mdb.Orders{TradeId: "root", OrderId: "root", Status: mdb.StatusWaitPay, Network: "tron", Token: "USDT", ReceiveAddress: "parent-wallet", Amount: 35, ActualAmount: 5, CallBackConfirm: mdb.CallBackConfirmOk}
			child := &mdb.Orders{TradeId: "child", OrderId: "child", ParentTradeId: root.TradeId, Status: mdb.StatusWaitPay, Network: "tron", Token: "USDT", ReceiveAddress: "child-wallet", Amount: 35, ActualAmount: 5, CallBackConfirm: mdb.CallBackConfirmOk}
			for _, o := range []*mdb.Orders{root, child} {
				if err := data.LockTransaction(o.Network, o.ReceiveAddress, o.Token, o.TradeId, 5, time.Minute); err != nil {
					t.Fatal(err)
				}
				if err := data.CreateOrderWithTransaction(dao.Mdb, o); err != nil {
					t.Fatal(err)
				}
			}
			switch failure {
			case "expired":
				if err := dao.Mdb.Model(root).Update("status", mdb.StatusExpired).Error; err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := dao.Mdb.Delete(root).Error; err != nil {
					t.Fatal(err)
				}
			case "write-failure":
				if err := dao.Mdb.Exec(`CREATE TRIGGER reject_parent_payment BEFORE UPDATE ON orders WHEN OLD.trade_id = 'root' AND NEW.status = 2 BEGIN SELECT RAISE(ABORT, 'injected write failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := OrderProcessing(&request.OrderProcessingRequest{TradeId: child.TradeId, Network: child.Network, Token: child.Token, ReceiveAddress: child.ReceiveAddress, Amount: 5, BlockTransactionId: "child-payment"}); err == nil {
				t.Fatal("parent failure must reject entire settlement")
			}
			got, err := data.GetOrderInfoByTradeId(child.TradeId)
			if err != nil || got.Status != mdb.StatusWaitPay || got.BlockTransactionId != "" || got.CallBackConfirm != mdb.CallBackConfirmOk {
				t.Fatalf("partial child settlement escaped rollback: %+v err=%v", got, err)
			}
			locked, err := data.GetTradeIdByWalletAddressAndAmountAndToken(child.Network, child.ReceiveAddress, child.Token, 5)
			if err != nil || locked != child.TradeId {
				t.Fatalf("failed settlement released child reservation: %s %v", locked, err)
			}
		})
	}
}

func TestReviewLegacySuccessNeverReleasesReassignedReservation(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	o := &mdb.Orders{TradeId: "old", OrderId: "old", Status: mdb.StatusWaitPay, Network: "tron", Token: "USDT", ReceiveAddress: "wallet", Amount: 35, ActualAmount: 5}
	if err := data.LockTransaction(o.Network, o.ReceiveAddress, o.Token, "new-owner", 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Create(o).Error; err != nil {
		t.Fatal(err)
	}
	if err := OrderProcessing(&request.OrderProcessingRequest{TradeId: o.TradeId, Network: o.Network, Token: o.Token, ReceiveAddress: o.ReceiveAddress, Amount: 5, BlockTransactionId: "old-payment"}); err != nil {
		t.Fatal(err)
	}
	locked, err := data.GetTradeIdByWalletAddressAndAmountAndToken(o.Network, o.ReceiveAddress, o.Token, 5)
	if err != nil || locked != "new-owner" {
		t.Fatalf("new owner's reservation removed: %s %v", locked, err)
	}
}

func TestReviewLegacyRecoveryRacesOrdinaryChildWithoutDoubleSettlement(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	root := &mdb.Orders{TradeId: "root", OrderId: "root", Status: mdb.StatusWaitPay, Network: "tron", Token: "USDT", ReceiveAddress: "parent-wallet", Amount: 35, ActualAmount: 5, CallBackConfirm: mdb.CallBackConfirmOk}
	child := &mdb.Orders{TradeId: "child", OrderId: "child", ParentTradeId: root.TradeId, Status: mdb.StatusWaitPay, Network: "tron", Token: "USDT", ReceiveAddress: "child-wallet", Amount: 35, ActualAmount: 5, CallBackConfirm: mdb.CallBackConfirmOk}
	for _, o := range []*mdb.Orders{root, child} {
		o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-time.Minute)))
		if err := dao.Mdb.Create(o).Error; err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, _, err := data.SettleLegacyObservedPaymentWithResult(data.ObservedPayment{Network: "tron", Token: "USDT", Address: root.ReceiveAddress, AssetID: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", RawAmount: big.NewInt(5_000_000), Decimals: 6, TxHash: "parent-payment", EventID: "contract:0", BlockTimeMs: now.UnixMilli()})
		if err != nil {
			t.Errorf("recovery: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		err := OrderProcessing(&request.OrderProcessingRequest{TradeId: child.TradeId, Network: child.Network, Token: child.Token, ReceiveAddress: child.ReceiveAddress, Amount: 5, BlockTransactionId: "child-payment"})
		if err != nil && err != constant.OrderStatusConflict {
			t.Errorf("ordinary settlement: %v", err)
		}
	}()
	close(start)
	wg.Wait()
	var concretePayments int64
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("status = ? AND block_transaction_id <> ''", mdb.StatusPaySuccess).Count(&concretePayments).Error; err != nil || concretePayments != 1 {
		t.Fatalf("multiple group payments: %d %v", concretePayments, err)
	}
	got, err := data.GetOrderInfoByTradeId(root.TradeId)
	if err != nil || got.Status != mdb.StatusPaySuccess || got.CallBackConfirm != mdb.CallBackConfirmNo {
		t.Fatalf("root callback lost: %+v %v", got, err)
	}
}

func TestReviewAptosLegacyFallbackKeepsPaymentBeforeChannelRefresh(t *testing.T) {
	for _, amount := range []int64{3, 7} {
		t.Run(fmt.Sprintf("paid-%d", amount), func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			now := time.Now()
			owner := "0x" + strings.Repeat("0", 63) + "a"
			assetID := ""
			if asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT"); err != nil {
				t.Fatal(err)
			} else if asset != nil {
				assetID = asset.ContractAddress
			}
			o := &mdb.Orders{TradeId: "fallback", OrderId: "fallback", Network: mdb.NetworkAptos, Token: "USDT", ReceiveAddress: owner, Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, NotifyUrl: "https://merchant.test/epay/notify"}
			o.CreatedAt = *carbon.NewTime(carbon.CreateFromStdTime(now.Add(-2 * time.Minute)))
			if err := data.LockTransaction(o.Network, owner, o.Token, o.TradeId, 5, time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := data.CreateOrderWithTransaction(dao.Mdb, o); err != nil {
				t.Fatal(err)
			}
			if err := data.RefreshOrderExpiration(o.TradeId); err != nil {
				t.Fatal(err)
			}
			transfer := MoveObservedTransfer{Network: mdb.NetworkAptos, ReceiveAddress: owner, Token: "USDT", AssetID: assetID, Amount: float64(amount), RawAmount: big.NewInt(amount * 1_000_000), Decimals: 6, TxID: "fallback-payment", TransferKey: "fallback-transfer", BlockTimeMs: now.Add(-time.Minute).UnixMilli()}
			gProcessedMoveTx.Delete(transfer.TransferKey)
			t.Cleanup(func() { gProcessedMoveTx.Delete(transfer.TransferKey) })
			for i := 0; i < 2; i++ {
				if err := ProcessMoveObservedTransferResult(transfer); err != nil {
					t.Fatal(err)
				}
			}
			got, err := data.GetOrderInfoByTradeId(o.TradeId)
			if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidAmount != float64(amount*7) || got.BlockTransactionId != transfer.TxID {
				t.Fatalf("fallback lost or misreported refreshed-channel payment: %+v %v", got, err)
			}
		})
	}
}
