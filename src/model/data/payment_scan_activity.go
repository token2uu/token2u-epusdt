package data

import (
	"fmt"
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Freeze before contacting a node. Initial outages may outlive order expiry;
// naturally expired orders therefore have no age limit in this bootstrap query.
func EnsureNetworkScanStartTime(network string, now time.Time) (time.Time, error) {
	key := network + ":bootstrap_time"
	stored, err := LoadPaymentCursor(key)
	if err != nil {
		return time.Time{}, err
	}
	if stored == "" {
		start := now.Add(-config.GetOrderExpirationTimeDuration() - 5*time.Minute)
		for _, spec := range []struct{ filter, order string }{
			{"assigned_at_ms > 0", "assigned_at_ms ASC"},
			{"assigned_at_ms = 0", "created_at ASC"},
		} {
			var oldest mdb.Orders
			err := dao.Mdb.Where("network = ? AND receive_address <> '' AND (pay_provider = ? OR pay_provider = '')", network, mdb.PaymentProviderOnChain).
				Where("status = ? OR (status = ? AND closed_reason = ?)", mdb.StatusWaitPay, mdb.StatusExpired, mdb.OrderClosedExpired).
				Where(spec.filter).Order(spec.order).Limit(1).Find(&oldest).Error
			if err != nil {
				return time.Time{}, err
			}
			if oldest.ID != 0 {
				assigned := time.UnixMilli(PaymentAssignmentTimeMs(&oldest))
				if assigned.Before(start) {
					start = assigned
				}
			}
		}
		row := mdb.PaymentScanCursor{Key: key, Position: start.UTC().Format(time.RFC3339Nano)}
		if err := dao.Mdb.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
			return time.Time{}, err
		}
		stored, err = LoadPaymentCursor(key)
		if err != nil {
			return time.Time{}, err
		}
	}
	start, err := time.Parse(time.RFC3339Nano, stored)
	if err != nil || start.UnixMilli() <= 0 || start.After(now) {
		return time.Time{}, fmt.Errorf("invalid %s bootstrap time", network)
	}
	return start, nil
}

// PaymentScanStartTime covers pending and naturally expired on-chain orders.
// The paid review grace controls scheduling, not historical payment ownership:
// an order may expire long before this address's first scanner round.
// Persist this before requesting a remote head so outages cannot move it forward.
func PaymentScanStartTime(network, address string, now time.Time) (time.Time, error) {
	start := now.Add(-config.GetOrderExpirationTimeDuration() - 5*time.Minute)
	for _, spec := range []struct{ filter, order string }{
		{"assigned_at_ms = 0", "created_at ASC"},
		{"assigned_at_ms > 0", "assigned_at_ms ASC"},
	} {
		var oldest mdb.Orders
		q := dao.Mdb.Where("network = ? AND (pay_provider = ? OR pay_provider = '')", network, mdb.PaymentProviderOnChain).
			Where("status = ? OR (status = ? AND closed_reason = ?)", mdb.StatusWaitPay, mdb.StatusExpired, mdb.OrderClosedExpired).
			Where(spec.filter)
		err := q.Where("receive_address = ?", address).Order(spec.order).First(&oldest).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return time.Time{}, err
		}
		if err == nil {
			assigned := time.UnixMilli(PaymentAssignmentTimeMs(&oldest))
			if assigned.Before(start) {
				start = assigned
			}
		}
	}
	return start, nil
}

// RecentlyActivePaymentAddresses includes pending payments, recently released
// addresses and recent receipts. Idle addresses must still be polled for late
// and unsolicited deposits; this set only selects the faster polling cadence.
func RecentlyActivePaymentAddresses(network string, now time.Time, recentFor time.Duration) (map[string]bool, error) {
	since := now.Add(-recentFor)
	var addresses []string
	err := dao.Mdb.Model(&mdb.Orders{}).
		Where("network = ? AND receive_address <> ''", network).
		Where(`(status = ? AND (payment_expires_at > ? OR
			(payment_expires_at IS NULL AND created_at > ?))) OR
			assigned_at_ms >= ? OR released_at_ms >= ? OR updated_at >= ?`,
			mdb.StatusWaitPay, now, now.Add(-config.GetOrderExpirationTimeDuration()),
			since.UnixMilli(), since.UnixMilli(), since).
		Distinct().Pluck("receive_address", &addresses).Error
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		out[strings.TrimSpace(address)] = true
	}
	addresses = nil
	if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).
		Where("network = ? AND created_at >= ?", network, since).
		Distinct().Pluck("address", &addresses).Error; err != nil {
		return nil, err
	}
	for _, address := range addresses {
		out[strings.TrimSpace(address)] = true
	}
	return out, nil
}
