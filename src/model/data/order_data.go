package data

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/dromara/carbon/v2"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrTransactionLocked = errors.New("transaction amount is already locked")

type PendingCallbackOrder struct {
	TradeId         string      `gorm:"column:trade_id"`
	CallbackNum     int         `gorm:"column:callback_num"`
	CallBackConfirm int         `gorm:"column:callback_confirm"`
	UpdatedAt       carbon.Time `gorm:"column:updated_at"`
}

func normalizeAmount(amount float64, precision int) (int64, string) {
	precision = NormalizeAmountPrecision(precision)
	value := decimal.NewFromFloat(amount).Round(int32(precision))
	return value.Shift(int32(precision)).IntPart(), value.StringFixed(int32(precision))
}

func normalizeLockAmount(amount float64) (int64, string) {
	return normalizeAmount(amount, GetAmountPrecision())
}

func normalizeLockNetwork(network string) string {
	return strings.ToLower(strings.TrimSpace(network))
}

func normalizeLockAddress(network, address string) string {
	address = strings.TrimSpace(address)
	network = normalizeLockNetwork(network)
	if isEVMNetwork(network) {
		return strings.ToLower(address)
	}
	if network == mdb.NetworkTon {
		if raw, err := addressutil.TonRawAddressKey(address); err == nil {
			return raw
		}
	}
	if network == mdb.NetworkAptos {
		if normalized, err := addressutil.NormalizeMoveAddress(address); err == nil {
			return normalized
		}
	}
	return address
}

func normalizeLockToken(token string) string {
	return strings.ToUpper(strings.TrimSpace(token))
}

func applyLockAddressFilter(tx *gorm.DB, network, address string) *gorm.DB {
	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	if isEVMNetwork(network) {
		return tx.Where("lower(address) = ?", address)
	}
	return tx.Where("address = ?", address)
}

func applyOrderReceiveAddressFilter(tx *gorm.DB, network, address string) *gorm.DB {
	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	if isEVMNetwork(network) {
		return tx.Where("lower(receive_address) = ?", address)
	}
	if network == mdb.NetworkTon {
		if parsed, err := addressutil.ParseTonMainnetAddress(address); err == nil {
			// Legacy rows store the friendly form; exclusive rows use raw keys.
			return tx.Where("(receive_address IN ? OR lower(receive_address) = ?)", []string{parsed.Display, parsed.Address.Bounce(true).String()}, parsed.Raw)
		}
	}
	return tx.Where("receive_address = ?", address)
}

func activeLocksForAddress(tx *gorm.DB, network, address, token string, now time.Time) *gorm.DB {
	query := tx.Model(&mdb.TransactionLock{}).
		Where("network = ?", normalizeLockNetwork(network)).
		Where("token = ?", normalizeLockToken(token)).
		Where("expires_at > ?", now)
	return applyLockAddressFilter(query, network, address)
}

func lockMatchesAmount(lock mdb.TransactionLock, amount float64) bool {
	precision := NormalizeAmountPrecision(lock.AmountPrecision)
	scaledAmount, _ := normalizeAmount(amount, precision)
	return lock.AmountScaled == scaledAmount
}

func lockAmountDecimal(lock mdb.TransactionLock) decimal.Decimal {
	if strings.TrimSpace(lock.AmountText) != "" {
		if value, err := decimal.NewFromString(lock.AmountText); err == nil {
			return value
		}
	}
	precision := NormalizeAmountPrecision(lock.AmountPrecision)
	return decimal.NewFromInt(lock.AmountScaled).Shift(int32(-precision))
}

// GetOrderInfoByOrderId fetches an order by merchant order id.
func GetOrderInfoByOrderId(orderId string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	err := dao.Mdb.Model(order).Limit(1).Find(order, "order_id = ?", orderId).Error
	return order, err
}

// GetOrderInfoByTradeId fetches an order by epusdt trade id.
func GetOrderInfoByTradeId(tradeId string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	err := dao.Mdb.Model(order).Limit(1).Find(order, "trade_id = ?", tradeId).Error
	return order, err
}

// Both precision and deadline belong to the actual reservation, not to settings
// read later while the business database is busy.
func orderReservation(order *mdb.Orders) (*mdb.TransactionLock, error) {
	var lock mdb.TransactionLock
	if err := dao.RuntimeDB.Where("trade_id = ?", order.TradeId).Find(&lock).Error; err != nil {
		return nil, err
	}
	if lock.ID == 0 || !lock.ExpiresAt.After(time.Now()) || lock.Network != normalizeLockNetwork(order.Network) ||
		lock.Address != normalizeLockAddress(order.Network, order.ReceiveAddress) || lock.Token != normalizeLockToken(order.Token) || !lockMatchesAmount(lock, order.ActualAmount) {
		return nil, constant.OrderStatusConflict
	}
	return &lock, nil
}

// CreateOrderWithTransaction creates an order in the active database transaction.
func CreateOrderWithTransaction(tx *gorm.DB, order *mdb.Orders) error {
	if order.CollectionMode != mdb.CollectionExclusive && order.ReceiveAddress != "" && (order.PayProvider == "" || order.PayProvider == mdb.PaymentProviderOnChain) {
		lock, err := orderReservation(order)
		if err != nil {
			return err
		}
		precision := NormalizeAmountPrecision(lock.AmountPrecision)
		order.AmountPrecision = &precision
		if order.PaymentExpiresAt == nil || order.PaymentExpiresAt.After(lock.ExpiresAt) {
			deadline := lock.ExpiresAt
			order.PaymentExpiresAt = &deadline
		}
		if order.AssignedAtMs == 0 && order.CreatedAt.StdTime().IsZero() {
			order.AssignedAtMs = time.Now().UnixMilli()
		}
	}
	if order.PaymentExpiresAt == nil {
		created := order.CreatedAt.StdTime()
		if created.IsZero() {
			created = time.Now()
		}
		deadline := created.Add(config.GetOrderExpirationTimeDuration())
		order.PaymentExpiresAt = &deadline
	}
	return tx.Model(order).Create(order).Error
}

// GetOrderByBlockIdWithTransaction fetches an order by blockchain tx id.
func GetOrderByBlockIdWithTransaction(tx *gorm.DB, blockID string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	err := tx.Model(order).Limit(1).Find(order, "block_transaction_id = ?", blockID).Error
	return order, err
}

// GetOrderByBlockTransactionIDs fetches the first order whose stored tx id
// matches any equivalent spelling supplied by the caller.
func GetOrderByBlockTransactionIDs(blockIDs []string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	seen := make(map[string]struct{}, len(blockIDs))
	candidates := make([]string, 0, len(blockIDs))
	for _, blockID := range blockIDs {
		blockID = strings.TrimSpace(blockID)
		if blockID == "" {
			continue
		}
		if _, ok := seen[blockID]; ok {
			continue
		}
		seen[blockID] = struct{}{}
		candidates = append(candidates, blockID)
	}
	if len(candidates) == 0 {
		return order, nil
	}
	err := dao.Mdb.Model(order).Where("block_transaction_id IN ?", candidates).Limit(1).Find(order).Error
	return order, err
}

// GetOrderByBlockTransactionIDsCaseInsensitive fetches the first order whose
// stored tx id matches any candidate after ASCII case folding. This is used
// only for hex-based chain tx ids; case-sensitive signatures must keep using
// GetOrderByBlockTransactionIDs.
func GetOrderByBlockTransactionIDsCaseInsensitive(blockIDs []string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	seen := make(map[string]struct{}, len(blockIDs))
	candidates := make([]string, 0, len(blockIDs))
	for _, blockID := range blockIDs {
		blockID = strings.ToLower(strings.TrimSpace(blockID))
		if blockID == "" {
			continue
		}
		if _, ok := seen[blockID]; ok {
			continue
		}
		seen[blockID] = struct{}{}
		candidates = append(candidates, blockID)
	}
	if len(candidates) == 0 {
		return order, nil
	}
	err := dao.Mdb.Model(order).Where("LOWER(block_transaction_id) IN ?", candidates).Limit(1).Find(order).Error
	return order, err
}

// SettleOrderGroupWithTransaction commits legacy payment, parent callback and
// sibling closure together. A failed parent write must roll back the child too.
// Callers serialize settlement with CollectionMu and release runtime locks only
// after this transaction commits, by the returned original trade IDs.
func SettleOrderGroupWithTransaction(tx *gorm.DB, req *request.OrderProcessingRequest, allowedStatuses, parentAllowedStatuses []int, authenticatedProvider bool) ([]mdb.Orders, error) {
	var order mdb.Orders
	if err := tx.Where("trade_id = ?", req.TradeId).First(&order).Error; err != nil {
		return nil, err
	}
	existing, err := GetOrderByBlockIdWithTransaction(tx, req.BlockTransactionId)
	if err != nil {
		return nil, err
	}
	if existing.ID > 0 {
		return nil, constant.OrderBlockAlreadyProcess
	}
	allowed := func(status int, statuses []int) bool {
		for _, candidate := range statuses {
			if candidate == status {
				return true
			}
		}
		return false
	}
	if !allowed(order.Status, allowedStatuses) {
		return nil, constant.OrderStatusConflict
	}
	root := &order
	if order.ParentTradeId != "" {
		root = &mdb.Orders{}
		if err := tx.Where("trade_id = ?", order.ParentTradeId).First(root).Error; err != nil {
			return nil, err
		}
		if !allowed(root.Status, parentAllowedStatuses) {
			// Only a verified provider callback can recover this migration-only
			// closure. Recheck its independent checkout inside the settlement tx.
			if !authenticatedProvider || root.Status != mdb.StatusExpired || root.ClosedReason != mdb.OrderClosedReservationUnknown ||
				root.BlockTransactionId != "" || root.PayBySubId != 0 || order.PayProvider != mdb.PaymentProviderOkPay ||
				order.Status != mdb.StatusWaitPay || order.BlockTransactionId != "" || order.PayBySubId != 0 ||
				!PaymentDeadline(&order).After(time.Now()) {
				return nil, constant.OrderStatusConflict
			}
			var provider mdb.ProviderOrder
			if err := tx.Where("trade_id = ? AND provider = ?", order.TradeId, order.PayProvider).Find(&provider).Error; err != nil {
				return nil, err
			}
			if provider.ID == 0 || provider.Status != mdb.ProviderOrderStatusPending || provider.ProviderOrderID == "" ||
				provider.ProviderOrderID != req.BlockTransactionId || !strings.EqualFold(provider.Coin, order.Token) ||
				provider.Amount <= 0 || provider.Amount != order.ActualAmount || req.Amount != order.ActualAmount {
				return nil, constant.OrderStatusConflict
			}
		}
	}
	paid := decimal.Zero
	if req.PaidAmount > 0 && req.PaidAmount != order.ActualAmount {
		if order.ActualAmount <= 0 || order.Amount <= 0 {
			return nil, fmt.Errorf("invalid settlement amount")
		}
		paid = decimal.NewFromFloat(req.PaidAmount).Mul(decimal.NewFromFloat(order.Amount)).Div(decimal.NewFromFloat(order.ActualAmount)).Round(2)
		if !paid.IsPositive() {
			return nil, fmt.Errorf("payment below settlement precision")
		}
	}
	crypto := req.Amount
	if req.PaidAmount > 0 {
		crypto = req.PaidAmount
	}
	if err := settleObservedPaymentTx(tx, &order, root, paid, decimal.NewFromFloat(crypto).String(), req.BlockTransactionId, time.Now()); err != nil {
		return nil, err
	}
	var group []mdb.Orders
	err = tx.Where("trade_id = ? OR parent_trade_id = ?", root.TradeId, root.TradeId).Find(&group).Error
	return group, err
}

// OrderSuccessWithTransaction marks an order as paid only if it is still waiting for payment.
func OrderSuccessWithTransaction(tx *gorm.DB, req *request.OrderProcessingRequest) (bool, error) {
	return OrderSuccessWithStatusesWithTransaction(tx, req, []int{mdb.StatusWaitPay})
}

// OrderSuccessWithStatusesWithTransaction marks an order as paid only if its
// current status is included in allowedStatuses.
func OrderSuccessWithStatusesWithTransaction(tx *gorm.DB, req *request.OrderProcessingRequest, allowedStatuses []int) (bool, error) {
	if len(allowedStatuses) == 0 {
		return false, nil
	}
	result := tx.Model(&mdb.Orders{}).
		Where("trade_id = ?", req.TradeId).
		Where("status IN ?", allowedStatuses).
		Updates(map[string]interface{}{
			"block_transaction_id": req.BlockTransactionId,
			"released_at_ms":       time.Now().UnixMilli(),
			"closed_reason":        "",
			"status":               mdb.StatusPaySuccess,
			"callback_confirm":     mdb.CallBackConfirmNo,
		})
	return result.RowsAffected > 0, result.Error
}

// OrderSuccessWithPaidAmount marks an order as paid, recording the fiat equivalent
// of the actual cryptocurrency amount paid. paidAmountCrypto is the actual crypto
// received; paid_amount is calculated as fiat based on the order's exchange rate.
func OrderSuccessWithPaidAmount(tx *gorm.DB, req *request.OrderProcessingRequest, allowedStatuses []int, paidAmountCrypto float64) (bool, error) {
	if len(allowedStatuses) == 0 {
		return false, nil
	}

	order := new(mdb.Orders)
	err := tx.Where("trade_id = ?", req.TradeId).First(order).Error
	if err != nil {
		return false, err
	}
	if order.ID == 0 {
		return false, nil
	}

	updates := map[string]interface{}{
		"block_transaction_id": req.BlockTransactionId,
		"released_at_ms":       time.Now().UnixMilli(),
		"closed_reason":        "",
		"status":               mdb.StatusPaySuccess,
		"callback_confirm":     mdb.CallBackConfirmNo,
	}

	if order.ActualAmount > 0 && paidAmountCrypto != order.ActualAmount {
		ratio := decimal.NewFromFloat(paidAmountCrypto).Div(decimal.NewFromFloat(order.ActualAmount))
		paidAmountFiat := decimal.NewFromFloat(order.Amount).Mul(ratio).Round(2)
		updates["paid_amount"] = paidAmountFiat.InexactFloat64()
	}

	result := tx.Model(&mdb.Orders{}).
		Where("trade_id = ?", req.TradeId).
		Where("status IN ?", allowedStatuses).
		Updates(updates)
	return result.RowsAffected > 0, result.Error
}

// expiredCallbackEnabledFrom is the earliest updated_at for which expired-order
// callbacks are dispatched. Orders that were expired before this feature shipped
// have older updated_at values and are silently skipped.
var expiredCallbackEnabledFrom = time.Date(2026, 9, 3, 0, 0, 0, 0, time.Local)

// GetPendingCallbackOrders returns the minimal callback scheduling state.
func GetPendingCallbackOrders(maxRetry int, limit int) ([]PendingCallbackOrder, error) {
	var orders []PendingCallbackOrder
	query := dao.Mdb.Model(&mdb.Orders{}).
		Select("trade_id", "callback_num", "callback_confirm", "updated_at").
		Where("callback_num <= ?", maxRetry).
		Where("callback_confirm = ?", mdb.CallBackConfirmNo).
		Where("status IN ?", []int{mdb.StatusPaySuccess, mdb.StatusExpired}).
		Where("updated_at >= ?", expiredCallbackEnabledFrom).
		Where("notify_url != ''").
		Order("updated_at asc")
	if limit > 0 {
		query = query.Limit(limit)
	}
	err := query.Find(&orders).Error
	return orders, err
}

// SaveCallBackOrdersResp persists a callback attempt result.
func SaveCallBackOrdersResp(order *mdb.Orders) error {
	return dao.Mdb.Model(order).
		Where("id = ?", order.ID).
		Where("callback_confirm = ?", mdb.CallBackConfirmNo).
		Where("status = ? AND block_transaction_id = ? AND callback_num = ?", order.Status, order.BlockTransactionId, order.CallbackNum).
		Updates(map[string]interface{}{
			"callback_num":     gorm.Expr("callback_num + ?", 1),
			"callback_confirm": order.CallBackConfirm,
		}).Error
}

// UpdateOrderIsExpirationById expires an order only if it is still pending and already timed out.
// When withCallback is true, callback_confirm is also set so the MQ worker dispatches a notification.
func UpdateOrderIsExpirationById(id uint64, expirationCutoff time.Time, withCallback bool) (bool, error) {
	var expired bool
	err := dao.Mdb.Transaction(func(tx *gorm.DB) error {
		var order mdb.Orders
		if err := tx.Where("id = ?", id).Find(&order).Error; err != nil {
			return err
		}
		if order.ID == 0 || (order.Status != mdb.StatusWaitPay && order.Status != mdb.StatusWaitSelect) {
			return nil
		}
		deadline := PaymentDeadline(&order)
		if order.PaymentExpiresAt != nil {
			if deadline.After(time.Now()) {
				return nil
			}
		} else if order.CreatedAt.StdTime().After(expirationCutoff) {
			return nil
		}
		updates := map[string]interface{}{
			"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedExpired,
			"released_at_ms": time.Now().UnixMilli(), "payment_expires_at": deadline,
		}
		if withCallback {
			updates["callback_confirm"] = mdb.CallBackConfirmNo
		}
		q := tx.Model(&mdb.Orders{}).Where("id = ? AND status = ?", id, order.Status)
		if order.PaymentExpiresAt == nil {
			q = q.Where("payment_expires_at IS NULL AND created_at <= ?", expirationCutoff)
		} else {
			q = q.Where("payment_expires_at <= ?", time.Now())
			delete(updates, "payment_expires_at")
		}
		result := q.Updates(updates)
		expired = result.RowsAffected > 0
		return result.Error
	})
	return expired, err
}

// CountActiveSubOrders counts sub-orders with status=WaitPay under a parent.
func CountActiveSubOrders(parentTradeId string) (int64, error) {
	var count int64
	err := dao.Mdb.Model(&mdb.Orders{}).
		Where("parent_trade_id = ?", parentTradeId).
		Where("status = ?", mdb.StatusWaitPay).
		Count(&count).Error
	return count, err
}

// CountSubOrders counts all sub-orders ever created under a parent.
func CountSubOrders(parentTradeId string) (int64, error) {
	var count int64
	err := dao.Mdb.Model(&mdb.Orders{}).
		Where("parent_trade_id = ?", parentTradeId).
		Count(&count).Error
	return count, err
}

// GetSubOrderByTokenNetwork finds an existing active sub-order matching token+network under a parent.
func GetSubOrderByTokenNetwork(parentTradeId string, token string, network string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	err := dao.Mdb.Model(order).
		Where("parent_trade_id = ?", parentTradeId).
		Where("token = ?", token).
		Where("network = ?", network).
		Where("status = ?", mdb.StatusWaitPay).
		Limit(1).
		Find(order).Error
	return order, err
}

// GetSiblingSubOrders returns active sub-orders under the same parent, excluding the given trade_id.
func GetSiblingSubOrders(parentTradeId string, excludeTradeId string) ([]mdb.Orders, error) {
	var orders []mdb.Orders
	err := dao.Mdb.Model(&mdb.Orders{}).
		Where("parent_trade_id = ?", parentTradeId).
		Where("trade_id != ?", excludeTradeId).
		Where("status = ?", mdb.StatusWaitPay).
		Find(&orders).Error
	return orders, err
}

// MarkParentOrderSuccess marks the parent order as paid and records which sub-order
// settled it. Only the status, callback_confirm, and pay_by_sub_id fields are updated;
// the parent's own block_transaction_id, actual_amount, and receive_address are
// intentionally left unchanged because the parent was not directly paid.
func MarkParentOrderSuccess(parentTradeId string, sub *mdb.Orders) (bool, error) {
	return MarkParentOrderSuccessWithTransaction(dao.Mdb, parentTradeId, sub)
}

// MarkParentOrderSuccessWithTransaction is the transactional variant of
// MarkParentOrderSuccess.
func MarkParentOrderSuccessWithTransaction(tx *gorm.DB, parentTradeId string, sub *mdb.Orders) (bool, error) {
	return MarkParentOrderSuccessWithStatusesWithTransaction(tx, parentTradeId, sub, []int{mdb.StatusWaitPay})
}

// MarkParentOrderSuccessWithStatusesWithTransaction marks a parent order as paid
// only if its current status is included in allowedStatuses.
func MarkParentOrderSuccessWithStatusesWithTransaction(tx *gorm.DB, parentTradeId string, sub *mdb.Orders, allowedStatuses []int) (bool, error) {
	if len(allowedStatuses) == 0 {
		return false, nil
	}
	result := tx.Model(&mdb.Orders{}).
		Where("trade_id = ?", parentTradeId).
		Where("status IN ?", allowedStatuses).
		Updates(map[string]interface{}{
			"status":           mdb.StatusPaySuccess,
			"callback_confirm": mdb.CallBackConfirmNo,
			"pay_by_sub_id":    sub.ID,
			"released_at_ms":   time.Now().UnixMilli(),
			"closed_reason":    "",
		})
	return result.RowsAffected > 0, result.Error
}

// ExpireSiblingSubOrdersWithTransaction expires all waiting sibling
// sub-orders under the same parent in one statement.
func ExpireSiblingSubOrdersWithTransaction(tx *gorm.DB, parentTradeId string, excludeTradeId string) error {
	return tx.Model(&mdb.Orders{}).
		Where("parent_trade_id = ?", parentTradeId).
		Where("trade_id != ?", excludeTradeId).
		Where("status = ?", mdb.StatusWaitPay).
		Updates(map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedSuperseded, "released_at_ms": time.Now().UnixMilli()}).Error
}

// MarkOrderSelected sets is_selected=true for the given trade_id.
func MarkOrderSelected(tradeId string) error {
	return dao.Mdb.Model(&mdb.Orders{}).
		Where("trade_id = ?", tradeId).
		Update("is_selected", true).Error
}

// CompleteWaitSelectOrder fills a placeholder order with concrete chain fields
// and makes it payable. It only updates status=WaitSelect rows so concurrent
// switch-network attempts cannot overwrite a payable order.
func CompleteWaitSelectOrder(tradeID string, network string, token string, receiveAddress string, actualAmount float64) (bool, error) {
	lock, err := orderReservation(&mdb.Orders{TradeId: tradeID, Network: network, Token: token, ReceiveAddress: receiveAddress, ActualAmount: actualAmount})
	if err != nil {
		return false, err
	}
	now := time.Now()
	result := dao.Mdb.Model(&mdb.Orders{}).
		Where("trade_id = ?", tradeID).
		Where("status = ?", mdb.StatusWaitSelect).
		Updates(map[string]interface{}{
			"status":             mdb.StatusWaitPay,
			"network":            strings.ToLower(strings.TrimSpace(network)),
			"token":              strings.ToUpper(strings.TrimSpace(token)),
			"receive_address":    receiveAddress,
			"actual_amount":      actualAmount,
			"amount_precision":   NormalizeAmountPrecision(lock.AmountPrecision),
			"is_selected":        false,
			"created_at":         now,
			"assigned_at_ms":     now.UnixMilli(),
			"payment_expires_at": lock.ExpiresAt,
		})
	return result.RowsAffected > 0, result.Error
}

// CompleteWaitSelectOkPayOrderWithTransaction converts a placeholder parent
// directly into an OkPay order. It intentionally does not create a local chain
// lock because OkPay owns the hosted payment target.
func CompleteWaitSelectOkPayOrderWithTransaction(tx *gorm.DB, tradeID string, token string, actualAmount float64) (bool, error) {
	result := tx.Model(&mdb.Orders{}).
		Where("trade_id = ?", tradeID).
		Where("status = ?", mdb.StatusWaitSelect).
		Updates(map[string]interface{}{
			"status":             mdb.StatusWaitPay,
			"network":            mdb.PaymentProviderOkPay,
			"token":              strings.ToUpper(strings.TrimSpace(token)),
			"receive_address":    "OKPAY",
			"actual_amount":      actualAmount,
			"is_selected":        false,
			"pay_provider":       mdb.PaymentProviderOkPay,
			"created_at":         gorm.Expr("CASE WHEN collection_mode = ? THEN created_at ELSE ? END", mdb.CollectionExclusive, time.Now()),
			"payment_expires_at": gorm.Expr("CASE WHEN collection_mode = ? THEN payment_expires_at ELSE ? END", mdb.CollectionExclusive, time.Now().Add(config.GetOrderExpirationTimeDuration())),
		})
	return result.RowsAffected > 0, result.Error
}

// MarkProviderSwitchParentSelectedWithTransaction marks a switch-network parent
// as selected for a hosted payment provider. It intentionally does not fill
// chain fields or create a transaction lock; the provider child order carries
// the concrete payment target.
func MarkProviderSwitchParentSelectedWithTransaction(tx *gorm.DB, tradeID string) (updated bool, err error) {
	err = tx.Transaction(func(tx *gorm.DB) error {
		var updateErr error
		updated, updateErr = refreshOrderExpirationTx(tx, tradeID, true)
		return updateErr
	})
	return
}

// ExpireOrderByTradeID marks a waiting order as expired. Used to retire failed
// child-order attempts that should not remain selectable/reusable.
func ExpireOrderByTradeID(tradeId string) error {
	return dao.Mdb.Model(&mdb.Orders{}).
		Where("trade_id = ?", tradeId).
		Where("status = ?", mdb.StatusWaitPay).
		Updates(map[string]interface{}{
			"status":         mdb.StatusExpired,
			"closed_reason":  mdb.OrderClosedSuperseded,
			"is_selected":    false,
			"released_at_ms": time.Now().UnixMilli(),
		}).Error
}

// RefreshOrderExpiration resets created_at to now so the expiration timer restarts.
// Called on the parent order when a sub-order is created or returned.
func RefreshOrderExpiration(tradeID string) error {
	return dao.Mdb.Transaction(func(tx *gorm.DB) error {
		return RefreshOrderExpirationWithTransaction(tx, tradeID)
	})
}

// Keep child creation and the parent deadline in the same business transaction.
func RefreshOrderExpirationWithTransaction(tx *gorm.DB, tradeID string) error {
	_, err := refreshOrderExpirationTx(tx, tradeID, false)
	return err
}

func refreshOrderExpirationTx(tx *gorm.DB, tradeID string, selected bool) (bool, error) {
	var order mdb.Orders
	if err := tx.Where("trade_id = ?", tradeID).First(&order).Error; err != nil {
		return false, err
	}
	if order.Status != mdb.StatusWaitPay && order.Status != mdb.StatusWaitSelect {
		return false, constant.OrderStatusConflict
	}
	updates := map[string]interface{}{}
	if selected {
		updates["status"], updates["is_selected"] = mdb.StatusWaitPay, true
	}
	if order.CollectionMode != mdb.CollectionExclusive {
		now := time.Now()
		if !PaymentDeadline(&order).After(now) {
			return false, constant.OrderStatusConflict
		}
		deadline := now.Add(config.GetOrderExpirationTimeDuration())
		if PaymentDeadline(&order).After(deadline) {
			deadline = PaymentDeadline(&order)
		}
		if order.ReceiveAddress != "" && (order.PayProvider == "" || order.PayProvider == mdb.PaymentProviderOnChain) {
			var lock mdb.TransactionLock
			if err := dao.RuntimeDB.Where("trade_id = ?", tradeID).Find(&lock).Error; err != nil {
				return false, err
			}
			if lock.ID == 0 || !lock.ExpiresAt.After(now) || lock.Network != normalizeLockNetwork(order.Network) ||
				lock.Address != normalizeLockAddress(order.Network, order.ReceiveAddress) || lock.Token != normalizeLockToken(order.Token) || !lockMatchesAmount(lock, order.ActualAmount) {
				return false, constant.OrderStatusConflict
			}
			// Runtime DB is separate: extend the reservation BEFORE publishing a
			// later deadline. If the business commit fails, only extra reservation
			// time remains; never release it early or resurrect an expired lock.
			if lock.ExpiresAt.Before(deadline) {
				result := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Where("id = ? AND trade_id = ? AND expires_at > ?", lock.ID, tradeID, now).Update("expires_at", deadline)
				if result.Error != nil {
					return false, result.Error
				}
				if result.RowsAffected != 1 {
					return false, constant.OrderStatusConflict
				}
			}
		}
		if err := freezeLegacyAssignment(tx, tradeID); err != nil {
			return false, err
		}
		updates["created_at"], updates["payment_expires_at"] = now, deadline
	}
	if len(updates) == 0 {
		return true, nil
	}
	result := tx.Model(&mdb.Orders{}).Where("id = ? AND status = ?", order.ID, order.Status).Updates(updates)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected != 1 {
		return false, constant.OrderStatusConflict
	}
	return true, nil
}

// Legacy channel switches refresh created_at for the checkout timer. Preserve
// the original address assignment first, so an earlier payment is still owned.
func freezeLegacyAssignment(tx *gorm.DB, tradeID string) error {
	var order mdb.Orders
	err := tx.Where("trade_id = ? AND collection_mode <> ? AND assigned_at_ms = 0 AND status = ? AND receive_address <> '' AND (pay_provider = ? OR pay_provider = '')", tradeID, mdb.CollectionExclusive, mdb.StatusWaitPay, mdb.PaymentProviderOnChain).Find(&order).Error
	if err != nil || order.ID == 0 {
		return err
	}
	return tx.Model(&mdb.Orders{}).Where("id = ? AND assigned_at_ms = 0 AND status = ?", order.ID, mdb.StatusWaitPay).UpdateColumn("assigned_at_ms", order.CreatedAt.TimestampMilli()).Error
}

// ResetCallbackConfirmOk sets callback_confirm back to Ok.
// Prevents the callback worker from retrying a sub-order with an empty notify_url.
func ResetCallbackConfirmOk(tradeId string) error {
	return dao.Mdb.Model(&mdb.Orders{}).
		Where("trade_id = ?", tradeId).
		Update("callback_confirm", mdb.CallBackConfirmOk).Error
}

// GetActiveSubOrders returns all active sub-orders under a parent.
func GetActiveSubOrders(parentTradeId string) ([]mdb.Orders, error) {
	var orders []mdb.Orders
	err := dao.Mdb.Model(&mdb.Orders{}).
		Where("parent_trade_id = ?", parentTradeId).
		Where("status = ?", mdb.StatusWaitPay).
		Find(&orders).Error
	return orders, err
}

// GetAllSubOrders returns all sub-orders (any status) under a parent order,
// ordered by creation time ascending.
func GetAllSubOrders(parentTradeId string) ([]mdb.Orders, error) {
	var orders []mdb.Orders
	err := dao.Mdb.Model(&mdb.Orders{}).
		Where("parent_trade_id = ?", parentTradeId).
		Order("created_at ASC").
		Find(&orders).Error
	return orders, err
}

// GetSubOrdersByParentTradeIds batch-fetches all sub-orders (any status)
// whose parent_trade_id is in the given set. Ordered by parent_trade_id
// then created_at ASC so callers can build a grouped map in one pass.
func GetSubOrdersByParentTradeIds(parentTradeIds []string) ([]mdb.Orders, error) {
	if len(parentTradeIds) == 0 {
		return nil, nil
	}
	var orders []mdb.Orders
	err := dao.Mdb.Model(&mdb.Orders{}).
		Where("parent_trade_id IN ?", parentTradeIds).
		Order("parent_trade_id ASC, created_at ASC").
		Find(&orders).Error
	return orders, err
}

// ExpireOrderByTradeId marks a single order as expired if still waiting.
func ExpireOrderByTradeId(tradeId string) error {
	return dao.Mdb.Model(&mdb.Orders{}).
		Where("trade_id = ?", tradeId).
		Where("status = ?", mdb.StatusWaitPay).
		Updates(map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedSuperseded, "released_at_ms": time.Now().UnixMilli()}).Error
}

// GetTradeIdByWalletAddressAndAmountAndToken resolves the reserved trade id by network, address, token and amount.
func GetTradeIdByWalletAddressAndAmountAndToken(network string, address string, token string, amount float64) (string, error) {
	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	var locks []mdb.TransactionLock
	err := activeLocksForAddress(dao.RuntimeDB, network, address, token, time.Now()).
		Order("created_at ASC").
		Find(&locks).Error
	if err != nil {
		return "", err
	}
	for _, lock := range locks {
		if lockMatchesAmount(lock, amount) {
			return lock.TradeId, nil
		}
	}
	return "", nil
}

// GetOnChainOrderByWalletAddressAndAmountAndTokenBeforeTime finds the most
// relevant on-chain order row when the runtime lock is gone. It is used by
// backfill scanners that need to recover a payment after the reservation lock
// has already expired.
func GetOnChainOrderByWalletAddressAndAmountAndTokenBeforeTime(network string, address string, token string, amount float64, before time.Time) (*mdb.Orders, error) {
	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	token = normalizeLockToken(token)

	order := new(mdb.Orders)
	query := dao.Mdb.Model(order).
		Where("network = ?", network).
		Where("token = ?", token).
		Where("actual_amount = ?", amount).
		Where("created_at <= ?", before).
		Where("status IN ?", []int{mdb.StatusWaitPay, mdb.StatusExpired}).
		Where("(pay_provider = ? OR pay_provider = '')", mdb.PaymentProviderOnChain)
	query = applyOrderReceiveAddressFilter(query, network, address)
	err := query.Order("created_at desc, id desc").Limit(1).Find(order).Error
	return order, err
}

// LockTransaction reserves a network+address+token+amount pair in sqlite until expiration.
func LockTransaction(network, address, token, tradeID string, amount float64, expirationTime time.Duration) error {
	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	precision := GetAmountPrecision()
	scaledAmount, amountText := normalizeAmount(amount, precision)
	normalizedToken := normalizeLockToken(token)
	now := time.Now()
	lock := &mdb.TransactionLock{
		Network:         network,
		Address:         address,
		Token:           normalizedToken,
		AmountScaled:    scaledAmount,
		AmountText:      amountText,
		AmountPrecision: precision,
		TradeId:         tradeID,
		ExpiresAt:       now.Add(expirationTime),
	}

	return dao.RuntimeDB.Transaction(func(tx *gorm.DB) error {
		expiredQuery := tx.Where("network = ?", network).
			Where("token = ?", normalizedToken).
			Where("expires_at <= ?", now)
		expiredQuery = applyLockAddressFilter(expiredQuery, network, address)
		if err := expiredQuery.Delete(&mdb.TransactionLock{}).Error; err != nil {
			return err
		}
		if err := tx.Where("trade_id = ?", tradeID).Delete(&mdb.TransactionLock{}).Error; err != nil {
			return err
		}

		var existing []mdb.TransactionLock
		if err := activeLocksForAddress(tx, network, address, normalizedToken, now).
			Find(&existing).Error; err != nil {
			return err
		}
		candidateAmount := lockAmountDecimal(*lock)
		for _, item := range existing {
			if lockAmountDecimal(item).Equal(candidateAmount) {
				return ErrTransactionLocked
			}
		}

		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(lock)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrTransactionLocked
		}
		return nil
	})
}

// UnLockTransaction releases the reservation for network+address+token+amount.
func UnLockTransaction(network string, address string, token string, amount float64) error {
	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	var locks []mdb.TransactionLock
	if err := activeLocksForAddress(dao.RuntimeDB, network, address, token, time.Now()).
		Find(&locks).Error; err != nil {
		return err
	}
	ids := make([]uint64, 0, len(locks))
	for _, lock := range locks {
		if lockMatchesAmount(lock, amount) {
			ids = append(ids, lock.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	return dao.RuntimeDB.Where("id IN ?", ids).Delete(&mdb.TransactionLock{}).Error
}

func UnLockTransactionByTradeId(tradeID string) error {
	return dao.RuntimeDB.Where("trade_id = ?", tradeID).Delete(&mdb.TransactionLock{}).Error
}

func CleanupExpiredTransactionLocks() error {
	return dao.RuntimeDB.Where("expires_at <= ?", time.Now()).Delete(&mdb.TransactionLock{}).Error
}

// TransactionMatchResult holds the result of matching a transfer to an order.
type TransactionMatchResult struct {
	TradeId        string
	IsEpayFallback bool
}

// IsEpayNotifyURL checks if the notify URL indicates an EPay-style order
// where amount mismatch is acceptable.
func IsEpayNotifyURL(notifyUrl string) bool {
	notifyUrl = strings.ToLower(strings.TrimSpace(notifyUrl))
	return strings.Contains(notifyUrl, "epay")
}

// IsPackageEpayNotifyURL checks if the notify URL indicates a package EPay
// order. Package EPay orders require exact amount match and do not participate
// in the EPay fallback matching path.
func IsPackageEpayNotifyURL(notifyUrl string) bool {
	notifyUrl = strings.ToLower(strings.TrimSpace(notifyUrl))
	return strings.HasSuffix(notifyUrl, "/api/user/package/epay/notify")
}

// GetTradeIdByWalletAddressAndAmountAndTokenWithEpayFallback first tries exact amount match.
// If no match is found, it falls back to any active EPay order at the same address.
func GetTradeIdByWalletAddressAndAmountAndTokenWithEpayFallback(network string, address string, token string, amount float64) (*TransactionMatchResult, error) {
	tradeID, err := GetTradeIdByWalletAddressAndAmountAndToken(network, address, token, amount)
	if err != nil {
		return nil, err
	}
	if tradeID != "" {
		return &TransactionMatchResult{TradeId: tradeID, IsEpayFallback: false}, nil
	}

	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	token = normalizeLockToken(token)
	now := time.Now()

	var locks []mdb.TransactionLock
	err = activeLocksForAddress(dao.RuntimeDB, network, address, token, now).
		Order("created_at ASC").
		Find(&locks).Error
	if err != nil {
		return nil, err
	}

	for _, lock := range locks {
		order, err := GetOrderInfoByTradeId(lock.TradeId)
		if err != nil {
			continue
		}
		if order.ID == 0 {
			continue
		}
		if order.Status != mdb.StatusWaitPay {
			continue
		}
		if !IsEpayNotifyURL(order.NotifyUrl) {
			continue
		}
		if IsPackageEpayNotifyURL(order.NotifyUrl) {
			continue
		}
		return &TransactionMatchResult{TradeId: lock.TradeId, IsEpayFallback: true}, nil
	}

	return &TransactionMatchResult{TradeId: "", IsEpayFallback: false}, nil
}

// FindEpayOrderByWalletAddressAndToken finds an active EPay order matching the
// wallet address and token. Used as a fallback when exact amount match fails.
func FindEpayOrderByWalletAddressAndToken(network string, address string, token string, amount float64) (*mdb.Orders, error) {
	network = normalizeLockNetwork(network)
	address = normalizeLockAddress(network, address)
	token = normalizeLockToken(token)

	order := new(mdb.Orders)
	query := dao.Mdb.Model(order).
		Where("network = ?", network).
		Where("token = ?", token).
		Where("status = ?", mdb.StatusWaitPay).
		Where("(pay_provider = ? OR pay_provider = '')", mdb.PaymentProviderOnChain)
	query = applyOrderReceiveAddressFilter(query, network, address)
	err := query.Order("created_at desc").Limit(1).Find(order).Error
	if err != nil {
		return nil, err
	}
	if order.ID == 0 {
		return nil, nil
	}
	if !IsEpayNotifyURL(order.NotifyUrl) {
		return nil, nil
	}
	if IsPackageEpayNotifyURL(order.NotifyUrl) {
		return nil, nil
	}
	return order, nil
}
