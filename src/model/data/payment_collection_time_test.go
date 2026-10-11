package data

import (
	"math/big"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func timedCollectionOrder(t *testing.T, network, id string, assigned int64) *mdb.Orders {
	t.Helper()
	expires := time.Now().Add(10 * time.Minute)
	o := &mdb.Orders{TradeId: id, OrderId: id, Network: network, Token: "USDT", Currency: "CNY",
		Amount: 35, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentType: mdb.PaymentTypeEpay,
		NotifyUrl: "https://merchant.test/notify", PaymentExpiresAt: &expires}
	if err := CreateExclusivePayment(o, mdb.ChainToken{ContractAddress: "usdt-contract", Decimals: 6}, []mdb.WalletAddress{{Address: "time-test-wallet"}}, false); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Model(o).Update("assigned_at_ms", assigned).Error; err != nil {
		t.Fatal(err)
	}
	return o
}

func timedCollectionReceipt(t *testing.T, o *mdb.Orders, timestamp int64) mdb.PaymentReceipt {
	t.Helper()
	p := ObservedPayment{Network: o.Network, Address: o.ReceiveAddress, Token: o.Token, AssetID: o.AssetID,
		TxHash: "time-test-tx", EventID: "0", RawAmount: big.NewInt(3_000_000), Decimals: 6, BlockTimeMs: timestamp}
	// Replaying either a successful or ambiguous receipt must remain idempotent.
	for i := 0; i < 2; i++ {
		if handled, err := ProcessExclusivePayment(p); !handled || err != nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	}
	var receipts []mdb.PaymentReceipt
	if err := dao.Mdb.Where("tx_hash = ?", p.TxHash).Find(&receipts).Error; err != nil || len(receipts) != 1 {
		t.Fatalf("receipts=%+v err=%v", receipts, err)
	}
	return receipts[0]
}

func assertTimedCollectionState(t *testing.T, o *mdb.Orders, paid bool) {
	t.Helper()
	got := reloadCollection(t, o.TradeId)
	var slot mdb.PaymentAddress
	if err := dao.Mdb.Where("network = ? AND address = ?", o.Network, o.ReceiveAddress).First(&slot).Error; err != nil {
		t.Fatal(err)
	}
	if paid {
		if got.Status != mdb.StatusPaySuccess || got.PaidAmount != 21 || got.PaidCryptoAmount != "3" || got.CallBackConfirm != mdb.CallBackConfirmNo || slot.TradeID != "" {
			t.Fatalf("valid 3 USDT payment not settled/released/queued: order=%+v slot=%+v", got, slot)
		}
	} else if got.Status != mdb.StatusWaitPay || got.PaidAmount != 0 || got.PaidCryptoAmount != "" || slot.TradeID != o.TradeId {
		t.Fatalf("unmatched payment changed new order or its reservation: order=%+v slot=%+v", got, slot)
	}
}

func TestExclusiveMillisecondAssignmentBoundary(t *testing.T) {
	for _, network := range []string{mdb.NetworkTron, mdb.NetworkAptos} {
		for _, tc := range []struct {
			name       string
			paidOffset int64
			paid       bool
		}{
			{"before_assignment_same_second", 100, false},
			{"at_assignment", 800, true},
			{"after_assignment_same_second", 900, true},
		} {
			t.Run(network+"/"+tc.name, func(t *testing.T) {
				defer testutil.SetupTestDatabases(t)()
				base := time.Now().Add(-5 * time.Second).Truncate(time.Second).UnixMilli()
				o := timedCollectionOrder(t, network, "new", base+800)
				r := timedCollectionReceipt(t, o, base+tc.paidOffset)
				assertTimedCollectionState(t, o, tc.paid)
				if !tc.paid && (r.Status != "exception" || r.Reason != "asset_or_assignment_mismatch" || r.TradeID != "") {
					t.Fatalf("pre-assignment payment was not left unmatched: %+v", r)
				}
			})
		}
	}
}

func TestExclusiveSecondPrecisionAssignmentBoundary(t *testing.T) {
	for _, network := range []string{mdb.NetworkSolana, mdb.NetworkTon, mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma} {
		for _, tc := range []struct {
			name                       string
			assignedOffset, paidOffset int64
			paid                       bool
		}{
			{"assignment_inside_payment_second", 800, 0, false},
			{"whole_second_after_assignment", -500, 0, true},
			{"assignment_at_second_start", 0, 0, true},
			{"payment_in_following_second", 800, 1000, true},
		} {
			t.Run(network+"/"+tc.name, func(t *testing.T) {
				defer testutil.SetupTestDatabases(t)()
				base := time.Now().Add(-5 * time.Second).Truncate(time.Second).UnixMilli()
				o := timedCollectionOrder(t, network, "new", base+tc.assignedOffset)
				r := timedCollectionReceipt(t, o, base+tc.paidOffset)
				assertTimedCollectionState(t, o, tc.paid)
				if !tc.paid && (r.Status != "exception" || r.Reason != "ambiguous_chain_time" || r.TradeID != "") {
					t.Fatalf("ambiguous payment was attributed to an order: %+v", r)
				}
			})
		}
	}
}

func TestExclusiveLateScanDoesNotCreditReassignedWallet(t *testing.T) {
	for _, network := range []string{mdb.NetworkTron, mdb.NetworkAptos, mdb.NetworkSolana, mdb.NetworkTon, mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma} {
		t.Run(network, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			base := time.Now().Add(-5 * time.Second).Truncate(time.Second).UnixMilli()
			old := timedCollectionOrder(t, network, "old", base-1000)
			if err := dao.Mdb.Model(old).Update("payment_expires_at", time.UnixMilli(base+500)).Error; err != nil {
				t.Fatal(err)
			}
			// Allocation expires the old order and immediately reuses its address.
			current := timedCollectionOrder(t, network, "new", base+800)
			paidAt := base
			wantReason, wantTrade := "ambiguous_chain_time", ""
			if network == mdb.NetworkTron || network == mdb.NetworkAptos {
				paidAt, wantReason, wantTrade = base+100, "closed_order", old.TradeId
			}
			r := timedCollectionReceipt(t, current, paidAt)
			assertTimedCollectionState(t, current, false)
			if got := reloadCollection(t, old.TradeId); got.Status != mdb.StatusExpired || got.PaidAmount != 0 {
				t.Fatalf("old order reopened: %+v", got)
			}
			if r.Status != "exception" || r.Reason != wantReason || r.TradeID != wantTrade {
				t.Fatalf("wrong historical attribution: %+v", r)
			}
		})
	}
}

func TestExclusiveSecondPrecisionExpirationBoundary(t *testing.T) {
	for _, tc := range []struct {
		name          string
		expiresOffset int64
		wantReason    string
	}{
		{"expires_inside_payment_second", 500, "ambiguous_chain_time"},
		{"expires_at_payment_second_start", 0, "late_payment"},
		{"whole_payment_second_before_expiry", 1000, "closed_order"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			base := time.Now().Add(-5 * time.Second).Truncate(time.Second).UnixMilli()
			o := timedCollectionOrder(t, mdb.NetworkSolana, "expired", base-1000)
			if err := dao.Mdb.Model(o).Update("payment_expires_at", time.UnixMilli(base+tc.expiresOffset)).Error; err != nil {
				t.Fatal(err)
			}
			r := timedCollectionReceipt(t, o, base)
			if r.Status != "exception" || r.Reason != tc.wantReason || r.TradeID != o.TradeId {
				t.Fatalf("wrong expiry-boundary receipt: %+v", r)
			}
			if got := reloadCollection(t, o.TradeId); got.Status != mdb.StatusExpired || got.PaidAmount != 0 {
				t.Fatalf("expired payment settled: %+v", got)
			}
		})
	}
}

func TestExclusiveExtraPaymentAfterSuccessDoesNotCreditNewOwner(t *testing.T) {
	for _, network := range []string{mdb.NetworkTron, mdb.NetworkSolana} {
		t.Run(network, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			base := time.Now().Add(-5 * time.Second).Truncate(time.Second).UnixMilli()
			old := timedCollectionOrder(t, network, "paid", base-2000)
			if handled, err := ProcessExclusivePayment(ObservedPayment{Network: network, Address: old.ReceiveAddress, Token: old.Token, AssetID: old.AssetID,
				TxHash: "first-payment", EventID: "0", RawAmount: big.NewInt(3_000_000), Decimals: 6, BlockTimeMs: base - 1000}); !handled || err != nil {
				t.Fatalf("initial settlement: handled=%v err=%v", handled, err)
			}
			assertTimedCollectionState(t, old, true)
			current := timedCollectionOrder(t, network, "new", base+800)
			paidAt, reason, tradeID := base, "ambiguous_chain_time", ""
			if network == mdb.NetworkTron {
				paidAt, reason, tradeID = base+100, "duplicate_payment", old.TradeId
			}
			r := timedCollectionReceipt(t, current, paidAt)
			assertTimedCollectionState(t, current, false)
			if r.Status != "exception" || r.Reason != reason || r.TradeID != tradeID {
				t.Fatalf("extra payment credited to wrong owner: %+v", r)
			}
			if got := reloadCollection(t, old.TradeId); got.Status != mdb.StatusPaySuccess || got.PaidAmount != 21 {
				t.Fatalf("first settlement changed: %+v", got)
			}
		})
	}
}
