package data

import (
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"gorm.io/gorm"
)

// PaymentScanStartTime covers both new payments and any older pending order.
// Persist this before requesting a remote head so outages cannot move it forward.
func PaymentScanStartTime(network, address string, now time.Time) (time.Time, error) {
	start := now.Add(-config.GetOrderExpirationTimeDuration() - 5*time.Minute)
	var oldest mdb.Orders
	err := dao.Mdb.Where("network = ? AND receive_address = ? AND status = ?", network, address, mdb.StatusWaitPay).
		Order("created_at ASC").First(&oldest).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return time.Time{}, err
	}
	if err == nil && oldest.CreatedAt.StdTime().Before(start) {
		start = oldest.CreatedAt.StdTime()
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
