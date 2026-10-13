package data

import (
	"fmt"
	"sort"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func PaymentDeadline(order *mdb.Orders) time.Time {
	if order.PaymentExpiresAt != nil {
		return *order.PaymentExpiresAt
	}
	return order.CreatedAt.StdTime().Add(config.GetOrderExpirationTimeDuration())
}

func PaymentAssignmentTimeMs(order *mdb.Orders) int64 {
	if order.AssignedAtMs > 0 {
		return order.AssignedAtMs
	}
	return order.CreatedAt.TimestampMilli()
}

func canSettleObservedOrder(order *mdb.Orders) bool {
	if order.Status == mdb.StatusWaitPay {
		return true
	}
	if order.Status != mdb.StatusExpired || order.BlockTransactionId != "" || order.PayBySubId != 0 {
		return false
	}
	// Older closed rows have no trustworthy cancellation provenance. Do not
	// infer natural expiry from updated_at (callbacks can update it too).
	return order.ClosedReason == mdb.OrderClosedExpired && order.PaymentExpiresAt != nil
}

// Losing the parent's old reservation retires that payment target, not a
// separately reserved child channel whose own payment evidence is valid.
func canSettleObservedParent(order *mdb.Orders) bool {
	return canSettleObservedOrder(order) || (order.Status == mdb.StatusExpired &&
		order.ClosedReason == mdb.OrderClosedReservationUnknown && order.BlockTransactionId == "" && order.PayBySubId == 0)
}

// ListPaymentReviewOrders includes a short post-expiry confirmation window.
// It never extends the payer's deadline. Every result is an on-chain payment.
func ListPaymentReviewOrders(network string, now time.Time) ([]mdb.Orders, error) {
	window := time.Duration(config.PaymentReviewWindowSeconds()) * time.Second
	grace := time.Duration(config.PaymentReviewGraceSeconds()) * time.Second
	var orders []mdb.Orders
	q := dao.Mdb.Where("network = ? AND receive_address <> '' AND token <> '' AND status IN ?", network,
		[]int{mdb.StatusWaitPay, mdb.StatusExpired}).
		Where("(pay_provider = ? OR pay_provider = '')", mdb.PaymentProviderOnChain).
		Where(`(payment_expires_at IS NOT NULL AND payment_expires_at >= ? AND payment_expires_at <= ?) OR
			(payment_expires_at IS NULL AND created_at >= ? AND created_at <= ?)`,
			now.Add(-grace), now.Add(window), now.Add(-grace-config.GetOrderExpirationTimeDuration()), now.Add(window-config.GetOrderExpirationTimeDuration()))
	if err := q.Order("created_at ASC, id ASC").Find(&orders).Error; err != nil {
		return nil, err
	}
	out := orders[:0]
	for _, order := range orders {
		if canSettleObservedOrder(&order) {
			out = append(out, order)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return PaymentDeadline(&out[i]).Before(PaymentDeadline(&out[j])) })
	return out, nil
}

// SettleLegacyObservedPaymentWithResult identifies ownership by chain time and
// settles pending or naturally expired orders together with a durable receipt.
// A verified historical owner must never fall through to current-lock matching:
// its reservation may already have expired or been reused before MQ catches up.
func SettleLegacyObservedPaymentWithResult(p ObservedPayment) (handled bool, settled *mdb.Orders, err error) {
	if p.RawAmount == nil || p.RawAmount.Sign() <= 0 || p.BlockTimeMs <= 0 || p.Decimals < 0 || p.Decimals > 36 || p.TxHash == "" || p.EventID == "" {
		return
	}
	p.Network, p.Token = normalizeLockNetwork(p.Network), normalizeLockToken(p.Token)
	p.TxHash = CanonicalPaymentTransactionID(p.Network, p.TxHash)
	p.Address = normalizeLockAddress(p.Network, p.Address)
	CollectionMu.Lock()
	defer CollectionMu.Unlock()
	err = dao.Mdb.Transaction(func(tx *gorm.DB) error {
		key := paymentEventKey(p.Network, p.TxHash, p.EventID)
		var receipt mdb.PaymentReceipt
		if err := tx.Where("event_key = ?", key).Find(&receipt).Error; err != nil {
			return err
		}
		if receipt.ID != 0 {
			// Exclusive exceptions may now be recoverable. Leave their replay
			// to the ownership-aware exclusive handler, including its evidence check.
			var original mdb.Orders
			if err := tx.Where("trade_id = ?", receipt.TradeID).Find(&original).Error; err != nil {
				return err
			}
			if original.ID != 0 && original.CollectionMode == mdb.CollectionExclusive {
				return nil
			}
			handled = true
			return nil
		}
		start, end := paymentChainTimeInterval(p)
		amount := decimal.NewFromBigInt(p.RawAmount, -int32(p.Decimals))
		candidates, ownershipEnds, err := legacyPaymentCandidates(tx, p)
		if err != nil {
			return err
		}
		receipt = mdb.PaymentReceipt{EventKey: key, Network: p.Network, Address: p.Address, Token: p.Token,
			AssetID: PaymentAssetID(p.Network, p.AssetID), TxHash: p.TxHash, EventID: p.EventID,
			RawAmount: p.RawAmount.String(), Decimals: p.Decimals, Amount: amount.String(), BlockTimeMs: p.BlockTimeMs,
			Status: "exception", Reason: "ambiguous_assignment"}
		if len(candidates) == 0 {
			// Absence of a payable window is not absence of received funds.
			// Keep late/unmatched payments to a historical legacy target for
			// manual reconciliation, without guessing a former order owner.
			var prior mdb.Orders
			q := tx.Select("id").Where("network = ? AND token = ? AND collection_mode <> ? AND (pay_provider = ? OR pay_provider = '')",
				p.Network, p.Token, mdb.CollectionExclusive, mdb.PaymentProviderOnChain).
				Where("(assigned_at_ms > 0 AND assigned_at_ms <= ?) OR (assigned_at_ms = 0 AND created_at <= ?)", end, time.UnixMilli(end))
			if err := applyOrderReceiveAddressFilter(q, p.Network, p.Address).Limit(1).Find(&prior).Error; err != nil {
				return err
			}
			if prior.ID == 0 {
				return nil
			}
			// An exclusive assignment has its own ownership/exception handler.
			var slots int64
			if err := tx.Model(&mdb.PaymentAddress{}).Where("network = ? AND address = ? AND (asset_id = ? OR token = ?)",
				p.Network, p.Address, receipt.AssetID, p.Token).Count(&slots).Error; err != nil {
				return err
			}
			if slots != 0 {
				return nil
			}
			handled = true
			receipt.Reason = "unassigned"
			return tx.Create(&receipt).Error
		}
		handled = true
		if len(candidates) != 1 {
			return tx.Create(&receipt).Error
		}
		order := &candidates[0]
		receipt.TradeID, receipt.ParentTradeID = order.TradeId, order.ParentTradeId
		var asset mdb.ChainToken
		if err := tx.Where("network = ? AND UPPER(symbol) = ? AND enabled = ?", p.Network, p.Token, true).Limit(1).Find(&asset).Error; err != nil {
			return err
		}
		if asset.ID != 0 {
			if PaymentAssetID(p.Network, asset.ContractAddress) != PaymentAssetID(p.Network, p.AssetID) || asset.Decimals != p.Decimals {
				receipt.Reason = "asset_mismatch"
				return tx.Create(&receipt).Error
			}
			if amount.LessThan(decimal.NewFromFloat(asset.MinAmount)) {
				receipt.Reason = "below_minimum"
				return tx.Create(&receipt).Error
			}
		}
		assigned := PaymentAssignmentTimeMs(order)
		if start < assigned || end >= ownershipEnds[order.ID] {
			receipt.Reason = "ambiguous_chain_time"
			return tx.Create(&receipt).Error
		}
		if !canSettleObservedOrder(order) {
			receipt.Reason = "closed_order"
			return tx.Create(&receipt).Error
		}
		root := order
		if order.ParentTradeId != "" {
			root = &mdb.Orders{}
			if err := tx.Where("trade_id = ?", order.ParentTradeId).First(root).Error; err != nil {
				return err
			}
			if !canSettleObservedParent(root) {
				receipt.Reason = "parent_closed"
				return tx.Create(&receipt).Error
			}
		}
		duplicate, err := PaymentEventAlreadyClaimed(tx, p.Network, p.TxHash, p.EventID)
		if err != nil {
			return err
		}
		if duplicate {
			receipt.Reason = "duplicate_payment"
			return tx.Create(&receipt).Error
		}
		paid := decimal.Zero
		if order.ActualAmount <= 0 || order.Amount <= 0 {
			return fmt.Errorf("invalid historical settlement amount")
		}
		// Ownership and settlement use the same persisted precision, including
		// the full-payment callback sentinel shared with manual verification.
		if !historicalAmountMatches(order, amount) {
			paid = amount.Mul(decimal.NewFromFloat(order.Amount)).Div(decimal.NewFromFloat(order.ActualAmount)).Round(2)
			if !paid.IsPositive() {
				receipt.Reason = "below_settlement_precision"
				return tx.Create(&receipt).Error
			}
		}
		if err := settleObservedPaymentTx(tx, order, root, paid, amount.String(), p.TxHash, time.Now()); err != nil {
			return err
		}
		receipt.Status, receipt.Reason = "settled", ""
		settled = &mdb.Orders{}
		if err := tx.Where("id = ?", order.ID).First(settled).Error; err != nil {
			return err
		}
		return tx.Create(&receipt).Error
	})
	if err != nil {
		settled = nil
	}
	return
}

func legacyOwnershipEnd(tx *gorm.DB, order *mdb.Orders, until, paymentEnd int64) (int64, error) {
	assigned := PaymentAssignmentTimeMs(order)
	for _, spec := range []struct {
		filter, sort string
		start, end   interface{}
	}{
		{"assigned_at_ms > ? AND assigned_at_ms <= ?", "assigned_at_ms ASC", assigned, paymentEnd},
		{"assigned_at_ms = 0 AND created_at >= ? AND created_at <= ?", "created_at ASC", time.UnixMilli(assigned + 1), time.UnixMilli(paymentEnd)},
	} {
		var next mdb.Orders
		q := tx.Where("id <> ? AND network = ? AND token = ? AND collection_mode <> ? AND actual_amount = ?", order.ID, order.Network, order.Token, mdb.CollectionExclusive, order.ActualAmount).
			Where("(pay_provider = ? OR pay_provider = '')", mdb.PaymentProviderOnChain).
			Where(spec.filter, spec.start, spec.end)
		if err := applyOrderReceiveAddressFilter(q, order.Network, order.ReceiveAddress).Order(spec.sort).Limit(1).Find(&next).Error; err != nil {
			return 0, err
		}
		if next.ID != 0 && PaymentAssignmentTimeMs(&next) < until {
			until = PaymentAssignmentTimeMs(&next)
		}
	}
	return until, nil
}

func historicalAmountMatches(order *mdb.Orders, amount decimal.Decimal) bool {
	expected := decimal.NewFromFloat(order.ActualAmount)
	if order.AmountPrecision == nil {
		return amount.Equal(expected)
	}
	precision := int32(NormalizeAmountPrecision(*order.AmountPrecision))
	return amount.Round(precision).Equal(expected.Round(precision))
}

// Both automatic recovery and manual amount fallback must select the same
// historical owner, including closed orders and exact-amount competitors.
func legacyPaymentCandidates(tx *gorm.DB, p ObservedPayment) ([]mdb.Orders, map[uint64]int64, error) {
	start, end := paymentChainTimeInterval(p)
	var history []mdb.Orders
	// Restrict the scan to windows that can intersect this payment. Reused
	// addresses can have years of orders; do not serialize all that history.
	q := tx.Where("network = ? AND token = ? AND collection_mode <> ?", p.Network, p.Token, mdb.CollectionExclusive).
		Where("(pay_provider = ? OR pay_provider = '')", mdb.PaymentProviderOnChain).
		Where("(assigned_at_ms > 0 AND assigned_at_ms <= ?) OR (assigned_at_ms = 0 AND created_at <= ?)", end, time.UnixMilli(end)).
		Where("released_at_ms = 0 OR released_at_ms > ?", start).
		Where("payment_expires_at > ? OR (payment_expires_at IS NULL AND created_at > ?)", time.UnixMilli(start), time.UnixMilli(start).Add(-config.GetOrderExpirationTimeDuration()))
	if err := applyOrderReceiveAddressFilter(q, p.Network, p.Address).Find(&history).Error; err != nil {
		return nil, nil, err
	}
	// Preserve pre-upgrade reservation evidence while it still exists. Orders
	// without either snapshot must use exact amounts, never today's precision.
	var missing []string
	for _, order := range history {
		if order.AmountPrecision == nil {
			missing = append(missing, order.TradeId)
		}
	}
	if len(missing) > 0 {
		var locks []mdb.TransactionLock
		if err := dao.RuntimeDB.Where("trade_id IN ?", missing).Find(&locks).Error; err != nil {
			return nil, nil, err
		}
		for i := range history {
			for _, lock := range locks {
				if history[i].AmountPrecision == nil && history[i].TradeId == lock.TradeId {
					precision := NormalizeAmountPrecision(lock.AmountPrecision)
					history[i].AmountPrecision = &precision
					if err := tx.Model(&mdb.Orders{}).Where("id = ? AND amount_precision IS NULL", history[i].ID).UpdateColumn("amount_precision", precision).Error; err != nil {
						return nil, nil, err
					}
				}
			}
		}
	}
	amount := decimal.NewFromBigInt(p.RawAmount, -int32(p.Decimals))
	// Migration ends legacy ownership at the first exclusive assignment.
	// Its historical timestamp, not the current slot owner, defines the cut.
	var exclusive mdb.Orders
	exclusiveQuery := tx.Where("network = ? AND token = ? AND collection_mode = ? AND assigned_at_ms > 0", p.Network, p.Token, mdb.CollectionExclusive)
	if err := applyOrderReceiveAddressFilter(exclusiveQuery, p.Network, p.Address).Order("assigned_at_ms ASC").Limit(1).Find(&exclusive).Error; err != nil {
		return nil, nil, err
	}
	var exactCandidates, fallbackCandidates []mdb.Orders
	ownershipEnds := make(map[uint64]int64)
	for _, order := range history {
		assigned := PaymentAssignmentTimeMs(&order)
		until := PaymentDeadline(&order).UnixMilli()
		if order.ReleasedAtMs > 0 && order.ReleasedAtMs < until {
			until = order.ReleasedAtMs
		}
		if exclusive.AssignedAtMs > 0 && exclusive.AssignedAtMs < until {
			until = exclusive.AssignedAtMs
		}
		// A later reservation of exactly the same slot proves release even
		// when an old refreshed parent still says pending. Include successors
		// that have themselves expired/closed; the filtered history omits them.
		var err error
		until, err = legacyOwnershipEnd(tx, &order, until, end)
		if err != nil {
			return nil, nil, err
		}
		if end < assigned || start >= until {
			continue
		}
		ownershipEnds[order.ID] = until
		exact := historicalAmountMatches(&order, amount)
		if !exact && (!IsEpayNotifyURL(order.NotifyUrl) || IsPackageEpayNotifyURL(order.NotifyUrl)) {
			continue
		}
		if exact {
			exactCandidates = append(exactCandidates, order)
		} else {
			fallbackCandidates = append(fallbackCandidates, order)
		}
	}
	candidates := exactCandidates
	if len(candidates) == 0 {
		candidates = fallbackCandidates
	}
	return candidates, ownershipEnds, nil
}
func paymentChainTimeInterval(p ObservedPayment) (start, end int64) {
	start, end = p.BlockTimeMs, p.BlockTimeMs
	if p.Network == mdb.NetworkSolana || p.Network == mdb.NetworkTon || isEVMNetwork(p.Network) {
		start = start / 1000 * 1000
		end = start + 999
	}
	return
}
