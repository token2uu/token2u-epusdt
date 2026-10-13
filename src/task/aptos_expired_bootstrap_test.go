package task

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
)

// Model the indexer's timestamp predicate as well as its event cursor.
type timedBootstrapDeposits struct {
	*fakeAptosDepositProvider
	paidAt time.Time
}

func (p *timedBootstrapDeposits) Deposits(ctx context.Context, owner string, after service.AptosEventPosition, head int64, limit int, since string) ([]service.AptosIndexedDeposit, error) {
	if since != "" {
		start, err := time.Parse(time.RFC3339Nano, since)
		if err != nil {
			return nil, err
		}
		if p.paidAt.Before(start) {
			return []service.AptosIndexedDeposit{}, nil
		}
	}
	return p.fakeAptosDepositProvider.Deposits(ctx, owner, after, head, limit, since)
}

func TestAptosFirstScanRecoversOldExpiredPayment(t *testing.T) {
	for _, paidReview := range []bool{false, true} {
		t.Run(fmt.Sprintf("paid_review_%t", paidReview), func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			ctx := context.Background()
			if paidReview {
				ctx = service.WithPaidPaymentReview(ctx)
			}
			owner := seedAptosScannerChain(t, "0xa")[0]
			asset, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkAptos, "USDT")
			if err != nil {
				t.Fatal(err)
			}
			expires := time.Now().Add(time.Hour)
			order := &mdb.Orders{TradeId: "old-aptos-payment", OrderId: "old-aptos-payment", Network: mdb.NetworkAptos, Token: "USDT", Amount: 35, ActualAmount: 5, Currency: "CNY", Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay, PaymentExpiresAt: &expires}
			if err := data.CreateExclusivePayment(order, *asset, []mdb.WalletAddress{{Address: owner}}, false); err != nil {
				t.Fatal(err)
			}
			assigned := time.Now().Add(-3 * time.Hour).Truncate(time.Millisecond)
			deadline := assigned.Add(10 * time.Minute)
			if err := dao.Mdb.Model(order).Updates(map[string]interface{}{"assigned_at_ms": assigned.UnixMilli(), "payment_expires_at": deadline}).Error; err != nil {
				t.Fatal(err)
			}
			if err := data.ExpireExclusivePayments(); err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(order).UpdateColumn("updated_at", deadline).Error; err != nil {
				t.Fatal(err)
			}
			p := &timedBootstrapDeposits{fakeAptosDepositProvider: &fakeAptosDepositProvider{head: 200, rows: map[string][]service.AptosIndexedDeposit{owner: {indexedDeposit(owner, asset.ContractAddress, 105, 1, "3000000")}}}, paidAt: assigned.Add(time.Minute)}
			if err := scanAptosOwnerDeposits(ctx, p, owner, []mdb.ChainToken{*asset}, p.head); err == nil {
				t.Fatal("missing historical transaction was silently skipped")
			}
			var tx map[string]interface{}
			if err := json.Unmarshal(aptosDepositTransaction(t, owner, asset.ContractAddress, 105), &tx); err != nil {
				t.Fatal(err)
			}
			tx["timestamp"] = fmt.Sprint(p.paidAt.UnixMicro())
			body, err := json.Marshal(tx)
			if err != nil {
				t.Fatal(err)
			}
			p.transactions = map[int64][]byte{105: body}
			if err := scanAptosOwnerDeposits(ctx, p, owner, []mdb.ChainToken{*asset}, p.head); err != nil {
				t.Fatal(err)
			}
			got, err := data.GetOrderInfoByTradeId(order.TradeId)
			if err != nil || got.Status != mdb.StatusPaySuccess || got.PaidCryptoAmount != "3" || got.PaidAmount != 21 || got.CallBackConfirm != mdb.CallBackConfirmNo {
				t.Fatalf("historical payment not settled: %+v %v", got, err)
			}
			if err := dao.Mdb.Model(order).Update("callback_confirm", mdb.CallBackConfirmOk).Error; err != nil {
				t.Fatal(err)
			}
			if err := saveAptosDepositCursorWithContext(ctx, owner, service.AptosEventPosition{Version: -1, Index: math.MaxInt64}); err != nil {
				t.Fatal(err)
			}
			if err := scanAptosOwnerDeposits(ctx, p, owner, []mdb.ChainToken{*asset}, p.head); err != nil {
				t.Fatal(err)
			}
			got, err = data.GetOrderInfoByTradeId(order.TradeId)
			var count int64
			if err != nil || got.CallBackConfirm != mdb.CallBackConfirmOk {
				t.Fatalf("replay reset acknowledgement: %+v %v", got, err)
			}
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 1 {
				t.Fatalf("receipt count=%d err=%v", count, err)
			}
		})
	}
}
