package dao

import (
	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/gookit/color"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"time"
)

var RuntimeDB *gorm.DB

func RuntimeInit() error {
	var err error
	runtimePath := config.GetRuntimeSqlitePath()
	color.Green.Printf("[runtime_db] sqlite filename: %s\n", runtimePath)
	RuntimeDB, err = openDB(runtimePath, &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
		Logger: logger.Default.LogMode(logger.Error),
	})
	if err != nil {
		color.Red.Printf("[runtime_db] sqlite open DB,err=%s\n", err)
		return err
	}

	concurrency := config.GetQueueConcurrency()
	if concurrency < 2 {
		concurrency = 2
	}
	if concurrency > 16 {
		concurrency = 16
	}
	if _, err = configureSQLite(RuntimeDB, concurrency); err != nil {
		color.Red.Printf("[runtime_db] sqlite connDB err:%s", err.Error())
		return err
	}
	if err = RuntimeDB.Exec("DROP INDEX IF EXISTS transaction_lock_token_amount_uindex").Error; err != nil {
		return err
	}
	if err = RuntimeDB.Exec("DROP INDEX IF EXISTS transaction_lock_address_token_amount_uindex").Error; err != nil {
		return err
	}
	if err = RuntimeDB.Exec("DROP INDEX IF EXISTS transaction_lock_network_address_token_amount_uindex").Error; err != nil {
		return err
	}
	if err = RuntimeDB.AutoMigrate(&mdb.TransactionLock{}); err != nil {
		color.Red.Printf("[runtime_db] sqlite migrate DB(TransactionLock),err=%s\n", err)
		return err
	}
	if err = RuntimeDB.AutoMigrate(&mdb.EvmScanCursor{}); err != nil {
		color.Red.Printf("[runtime_db] sqlite migrate DB(EvmScanCursor),err=%s\n", err)
		return err
	}
	if err = snapshotLegacyReservations(); err != nil {
		return err
	}

	log.Sugar.Debug("[runtime_db] sqlite connDB success")
	return nil
}

// Run before workers can delete expired locks. The source precision belongs to
// the reservation, so a changed global setting must never rewrite this history.
func snapshotLegacyReservations() error {
	var locks []mdb.TransactionLock
	err := RuntimeDB.FindInBatches(&locks, 200, func(_ *gorm.DB, _ int) error {
		return Mdb.Transaction(func(tx *gorm.DB) error {
			for _, lock := range locks {
				var order mdb.Orders
				if err := tx.Where("trade_id = ? AND collection_mode <> ?", lock.TradeId, mdb.CollectionExclusive).Find(&order).Error; err != nil {
					return err
				}
				if order.ID == 0 {
					continue
				}
				updates := map[string]interface{}{}
				if order.AmountPrecision == nil && lock.AmountPrecision >= 2 && lock.AmountPrecision <= 6 {
					updates["amount_precision"] = lock.AmountPrecision
				}
				if order.Status == mdb.StatusWaitPay && (order.PayProvider == "" || order.PayProvider == mdb.PaymentProviderOnChain) && !lock.ExpiresAt.IsZero() {
					// Old channel switches refreshed created_at without extending the
					// lock. Preserve its original interval before cleanup deletes it.
					if order.PaymentExpiresAt == nil || order.PaymentExpiresAt.After(lock.ExpiresAt) {
						updates["payment_expires_at"] = lock.ExpiresAt
					}
					if order.AssignedAtMs == 0 && !lock.CreatedAt.IsZero() && lock.CreatedAt.Before(order.CreatedAt.StdTime()) {
						updates["assigned_at_ms"] = lock.CreatedAt.UnixMilli()
					}
				}
				if len(updates) == 0 {
					continue
				}
				if err := tx.Model(&mdb.Orders{}).Where("id = ?", order.ID).UpdateColumns(updates).Error; err != nil {
					return err
				}
			}
			return nil
		})
	}).Error
	if err != nil {
		return err
	}
	// Pre-upgrade rows without any surviving reservation evidence cannot
	// inherit a trustworthy interval from a possibly refreshed created_at.
	// Retire these unreserved checkout targets; historical payments need review.
	var orders []mdb.Orders
	return Mdb.Where("collection_mode <> ? AND status = ? AND amount_precision IS NULL AND receive_address <> '' AND (pay_provider = ? OR pay_provider = '')",
		mdb.CollectionExclusive, mdb.StatusWaitPay, mdb.PaymentProviderOnChain).FindInBatches(&orders, 200, func(tx *gorm.DB, _ int) error {
		for _, order := range orders {
			var count int64
			if err := RuntimeDB.Model(&mdb.TransactionLock{}).Where("trade_id = ?", order.TradeId).Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				continue
			}
			updates := map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedReservationUnknown,
				"released_at_ms": time.Now().UnixMilli(), "callback_confirm": mdb.CallBackConfirmNo}
			if order.ParentTradeId != "" {
				updates["callback_confirm"] = mdb.CallBackConfirmOk
			}
			if err := tx.Model(&mdb.Orders{}).Where("id = ? AND status = ?", order.ID, mdb.StatusWaitPay).Updates(updates).Error; err != nil {
				return err
			}
		}
		return nil
	}).Error
}
