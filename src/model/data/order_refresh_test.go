package data

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/util/constant"
	"gorm.io/gorm"
)

func TestOrderRefreshKeepsReservationUntilNewDeadline(t *testing.T) {
	for _, providerSwitch := range []bool{false, true} {
		t.Run(fmt.Sprint(providerSwitch), func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			now := time.Now()
			old := reviewLegacy(t, "refreshed-parent", mdb.StatusWaitPay, 5, now.Add(-10*time.Minute+300*time.Millisecond), now.Add(300*time.Millisecond), "")
			if err := LockTransaction(old.Network, old.ReceiveAddress, old.Token, old.TradeId, 5, 300*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if providerSwitch {
				if updated, err := MarkProviderSwitchParentSelectedWithTransaction(dao.Mdb, old.TradeId); err != nil || !updated {
					t.Fatalf("refresh: %v %v", updated, err)
				}
			} else if err := RefreshOrderExpiration(old.TradeId); err != nil {
				t.Fatal(err)
			}
			renewed, _ := GetOrderInfoByTradeId(old.TradeId)
			var lock mdb.TransactionLock
			if err := dao.RuntimeDB.Where("trade_id = ?", old.TradeId).First(&lock).Error; err != nil {
				t.Fatal(err)
			}
			if lock.ExpiresAt.Before(PaymentDeadline(renewed)) {
				t.Fatalf("lock expires before displayed deadline: %v < %v", lock.ExpiresAt, PaymentDeadline(renewed))
			}
			time.Sleep(time.Until(now.Add(350 * time.Millisecond)))
			if err := CleanupExpiredTransactionLocks(); err != nil {
				t.Fatal(err)
			}
			if err := LockTransaction("tron", "wallet", "USDT", "new-owner", 5, time.Minute); !errors.Is(err, ErrTransactionLocked) {
				t.Fatalf("renewed reservation was reassigned: %v", err)
			}
			// A payment during the renewed interval must still settle the original order.
			p := reviewPayment(time.Now())
			handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
			if err != nil || !handled || settled == nil || settled.TradeId != old.TradeId {
				t.Fatalf("renewed payment rejected: %v %+v %v", handled, settled, err)
			}
			if err := UnLockTransactionByTradeId(old.TradeId); err != nil {
				t.Fatal(err)
			}
			fresh := reviewLegacy(t, "new-owner", mdb.StatusWaitPay, 5, time.Now(), time.Now().Add(time.Minute), "")
			if err := LockTransaction("tron", "wallet", "USDT", fresh.TradeId, 5, time.Minute); err != nil {
				t.Fatal(err)
			}
			p = reviewPayment(time.Now().Add(time.Second))
			p.TxHash = "new-owner-payment"
			handled, settled, err = SettleLegacyObservedPaymentWithResult(p)
			if err != nil || !handled || settled == nil || settled.TradeId != fresh.TradeId {
				t.Fatalf("released history blocked new payment: %v %+v %v", handled, settled, err)
			}
		})
	}
}

func TestOrderRefreshDoesNotResurrectLostOrExpiredReservations(t *testing.T) {
	for _, kind := range []string{"missing", "expired", "reassigned", "order-expired"} {
		t.Run(kind, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			now := time.Now()
			expires := now.Add(time.Minute)
			if kind == "order-expired" {
				expires = now.Add(-time.Second)
			}
			old := reviewLegacy(t, "old", mdb.StatusWaitPay, 5, now.Add(-time.Minute), expires, "")
			if kind != "missing" {
				ttl := time.Minute
				if kind == "expired" || kind == "reassigned" {
					ttl = -time.Minute
				}
				if err := LockTransaction("tron", "wallet", "USDT", old.TradeId, 5, ttl); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "reassigned" {
				if err := LockTransaction("tron", "wallet", "USDT", "other", 5, time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			if err := RefreshOrderExpiration(old.TradeId); !errors.Is(err, constant.OrderStatusConflict) {
				t.Fatalf("lost reservation renewed: %v", err)
			}
			got, _ := GetOrderInfoByTradeId(old.TradeId)
			if !PaymentDeadline(got).Equal(expires) || got.AssignedAtMs != 0 {
				t.Fatalf("failed refresh changed history: %+v", got)
			}
			if kind == "reassigned" {
				id, err := GetTradeIdByWalletAddressAndAmountAndToken("tron", "wallet", "USDT", 5)
				if err != nil || id != "other" {
					t.Fatalf("new reservation changed: %s %v", id, err)
				}
			}
		})
	}
}

func TestOrderRefreshBusinessFailureNeverReleasesReservationEarly(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	old := reviewLegacy(t, "rollback", mdb.StatusWaitPay, 5, now.Add(-time.Minute), now.Add(time.Minute), "")
	if err := LockTransaction("tron", "wallet", "USDT", old.TradeId, 5, time.Minute); err != nil {
		t.Fatal(err)
	}
	hook := "review_refresh_failure"
	if err := dao.Mdb.Callback().Update().Before("gorm:update").Register(hook, func(tx *gorm.DB) {
		if tx.Statement.Table == "orders" {
			tx.AddError(errors.New("injected business write failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer dao.Mdb.Callback().Update().Remove(hook)
	if err := RefreshOrderExpiration(old.TradeId); err == nil {
		t.Fatal("fixture did not fail")
	}
	got, _ := GetOrderInfoByTradeId(old.TradeId)
	if !PaymentDeadline(got).Equal(PaymentDeadline(old)) {
		t.Fatal("failed transaction extended business deadline")
	}
	var lock mdb.TransactionLock
	if err := dao.RuntimeDB.Where("trade_id = ?", old.TradeId).First(&lock).Error; err != nil {
		t.Fatal(err)
	}
	if lock.ExpiresAt.Before(PaymentDeadline(got)) {
		t.Fatal("failure left deadline unreserved")
	}
}

func TestReservationDeadlineForPlaceholderAndInvalidLocks(t *testing.T) {
	for _, kind := range []string{"valid", "missing", "expired", "wrong-owner"} {
		t.Run(kind, func(t *testing.T) {
			defer testutil.SetupTestDatabases(t)()
			o := &mdb.Orders{TradeId: "placeholder", OrderId: "placeholder", Status: mdb.StatusWaitSelect, Amount: 35}
			if err := CreateOrderWithTransaction(dao.Mdb, o); err != nil {
				t.Fatal(err)
			}
			if kind != "missing" {
				ttl := time.Minute
				id := o.TradeId
				if kind == "expired" {
					ttl = -time.Second
				}
				if kind == "wrong-owner" {
					id = "other"
				}
				if err := LockTransaction("tron", "wallet", "USDT", id, 5, ttl); err != nil {
					t.Fatal(err)
				}
			}
			updated, err := CompleteWaitSelectOrder(o.TradeId, "tron", "USDT", "wallet", 5)
			got, _ := GetOrderInfoByTradeId(o.TradeId)
			if kind == "valid" {
				if err != nil || !updated {
					t.Fatalf("valid assignment rejected: %v", err)
				}
				var lock mdb.TransactionLock
				if err := dao.RuntimeDB.Where("trade_id = ?", o.TradeId).First(&lock).Error; err != nil {
					t.Fatal(err)
				}
				if !PaymentDeadline(got).Equal(lock.ExpiresAt) {
					t.Fatal("placeholder deadline exceeds original reservation")
				}
			} else if err == nil || updated || got.Status != mdb.StatusWaitSelect {
				t.Fatalf("invalid reservation made payable: %+v %v", got, err)
			}
		})
	}
}

func TestLostLegacyParentDoesNotBlockValidChildPayment(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	now := time.Now()
	parent := reviewLegacy(t, "lost-parent", mdb.StatusExpired, 5, now.Add(-2*time.Minute), now.Add(time.Minute), "")
	if err := dao.Mdb.Model(parent).Update("closed_reason", mdb.OrderClosedReservationUnknown).Error; err != nil {
		t.Fatal(err)
	}
	child := reviewLegacy(t, "valid-child", mdb.StatusWaitPay, 5, now.Add(-time.Minute), now.Add(time.Minute), "")
	if err := dao.Mdb.Model(child).Updates(map[string]interface{}{"parent_trade_id": parent.TradeId, "receive_address": "child-wallet"}).Error; err != nil {
		t.Fatal(err)
	}
	p := reviewPayment(now)
	p.Address = "child-wallet"
	handled, settled, err := SettleLegacyObservedPaymentWithResult(p)
	if err != nil || !handled || settled == nil || settled.TradeId != child.TradeId {
		t.Fatalf("valid child payment blocked by lost parent reservation: %v %+v %v", handled, settled, err)
	}
	got, _ := GetOrderInfoByTradeId(parent.TradeId)
	if got.Status != mdb.StatusPaySuccess || got.PayBySubId != child.ID {
		t.Fatalf("parent not settled by child: %+v", got)
	}
}
