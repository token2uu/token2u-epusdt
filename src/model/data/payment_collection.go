package data

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CollectionMu serializes the collection state machine in the supported
// single-process deployment. Database constraints are additional safeguards.
var CollectionMu sync.Mutex

func PaymentAssetID(network, asset string) string {
	asset = strings.TrimSpace(asset)
	if asset == "" {
		return "native"
	}
	if isEVMNetwork(network) || network == mdb.NetworkAptos {
		return strings.ToLower(asset)
	}
	return asset
}

// CreateExclusivePayment persists an assignment together with its order. For
// placeholders update=true fills the original order without changing CreatedAt.
func CreateExclusivePayment(order *mdb.Orders, token mdb.ChainToken, wallets []mdb.WalletAddress, update bool) error {
	if order == nil || order.ActualAmount <= 0 || order.Amount <= 0 || order.PaymentExpiresAt == nil || token.Decimals < 0 || token.Decimals > 36 {
		return constant.PayAmountErr
	}
	// The displayed amount must be payable under the same minimum snapshot
	// used for settlement. Reject before allocating a slot or updating a parent.
	if decimal.NewFromFloat(order.ActualAmount).LessThan(decimal.NewFromFloat(token.MinAmount)) {
		return constant.PayAmountErr
	}
	CollectionMu.Lock()
	defer CollectionMu.Unlock()
	now := time.Now()
	assetID := PaymentAssetID(order.Network, token.ContractAddress)
	return dao.Mdb.Transaction(func(tx *gorm.DB) error {
		if err := expireExclusiveTx(tx, now); err != nil {
			return err
		}
		if order.ParentTradeId != "" {
			var parent mdb.Orders
			if err := tx.Where("trade_id = ?", order.ParentTradeId).First(&parent).Error; err != nil {
				return err
			}
			if parent.Status != mdb.StatusWaitPay {
				return constant.OrderStatusConflict
			}
			order.PaymentExpiresAt = parent.PaymentExpiresAt
			order.IsSelected = true
			// Selecting another network confirms both channels. Commit the
			// parent's flag together with child creation and address allocation.
			if err := tx.Model(&parent).Update("is_selected", true).Error; err != nil {
				return err
			}
		}
		var slots []mdb.PaymentAddress
		if err := tx.Where("network = ? AND asset_id = ?", order.Network, assetID).Find(&slots).Error; err != nil {
			return err
		}
		byAddress := make(map[string]mdb.PaymentAddress)
		for _, slot := range slots {
			byAddress[slot.Address] = slot
		}
		sort.SliceStable(wallets, func(i, j int) bool {
			a := byAddress[normalizeLockAddress(order.Network, wallets[i].Address)]
			b := byAddress[normalizeLockAddress(order.Network, wallets[j].Address)]
			if (a.ID == 0) != (b.ID == 0) {
				return a.ID == 0
			}
			if a.LastReleasedAt != b.LastReleasedAt {
				return a.LastReleasedAt < b.LastReleasedAt
			}
			return wallets[i].ID < wallets[j].ID
		})
		for _, wallet := range wallets {
			address := normalizeLockAddress(order.Network, wallet.Address)
			slot := byAddress[address]
			if slot.TradeID != "" {
				continue
			}
			// Legacy in-flight orders reserve the entire address/token during
			// migration. Never overlay an exclusive payment on an amount lock.
			var oldLocks int64
			if err := activeLocksForAddress(dao.RuntimeDB, order.Network, address, order.Token, now).Count(&oldLocks).Error; err != nil {
				return err
			}
			if oldLocks != 0 {
				continue
			}
			if slot.ID == 0 {
				slot = mdb.PaymentAddress{Network: order.Network, Address: address, Token: order.Token, AssetID: assetID, TradeID: order.TradeId}
				if err := tx.Create(&slot).Error; err != nil {
					return err
				}
			} else {
				r := tx.Model(&mdb.PaymentAddress{}).Where("id = ? AND trade_id = ?", slot.ID, "").Update("trade_id", order.TradeId)
				if r.Error != nil {
					return r.Error
				}
				if r.RowsAffected != 1 {
					continue
				}
			}
			order.CollectionMode = mdb.CollectionExclusive
			order.ReceiveAddress = address
			order.AssignedAtMs = now.UnixMilli()
			order.AssetID = assetID
			order.AssetDecimals = token.Decimals
			order.MinimumPayment = decimal.NewFromFloat(token.MinAmount).String()
			order.FiatPerToken = decimal.NewFromFloat(order.Amount).Div(decimal.NewFromFloat(order.ActualAmount)).String()
			if update {
				r := tx.Model(&mdb.Orders{}).Where("trade_id = ? AND status = ?", order.TradeId, mdb.StatusWaitSelect).Updates(map[string]interface{}{
					"network": order.Network, "token": order.Token, "receive_address": address, "actual_amount": order.ActualAmount,
					"collection_mode": order.CollectionMode, "assigned_at_ms": order.AssignedAtMs, "asset_id": order.AssetID,
					"asset_decimals": order.AssetDecimals, "minimum_payment": order.MinimumPayment, "fiat_per_token": order.FiatPerToken,
					"payment_expires_at": order.PaymentExpiresAt, "status": mdb.StatusWaitPay, "is_selected": true,
				})
				if r.Error != nil {
					return r.Error
				}
				if r.RowsAffected != 1 {
					return constant.OrderStatusConflict
				}
				return tx.Where("trade_id = ?", order.TradeId).First(order).Error
			}
			return tx.Create(order).Error
		}
		return constant.NotAvailableWalletAddress
	})
}

// SelectExclusivePayment confirms an existing channel and its parent atomically.
// Selection must not extend expiry, change ownership, or reopen a closed order.
func SelectExclusivePayment(tradeID string) (*mdb.Orders, error) {
	CollectionMu.Lock()
	defer CollectionMu.Unlock()
	var selected mdb.Orders
	err := dao.Mdb.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("trade_id = ?", tradeID).First(&selected).Error; err != nil {
			return err
		}
		ids := []string{selected.TradeId}
		if selected.ParentTradeId != "" {
			ids = append(ids, selected.ParentTradeId)
		}
		var orders []mdb.Orders
		if err := tx.Where("trade_id IN ?", ids).Find(&orders).Error; err != nil {
			return err
		}
		if len(orders) != len(ids) {
			return constant.OrderStatusConflict
		}
		now := time.Now()
		for _, order := range orders {
			if order.CollectionMode != mdb.CollectionExclusive || order.Status != mdb.StatusWaitPay || order.PaymentExpiresAt == nil || !order.PaymentExpiresAt.After(now) {
				return constant.OrderStatusConflict
			}
		}
		return tx.Model(&mdb.Orders{}).Where("trade_id IN ?", ids).Update("is_selected", true).Error
	})
	if err != nil {
		return nil, err
	}
	selected.IsSelected = true
	return &selected, nil
}

func releaseExclusiveTx(tx *gorm.DB, tradeID string, now time.Time) error {
	if err := tx.Model(&mdb.PaymentAddress{}).Where("trade_id = ?", tradeID).Updates(map[string]interface{}{"trade_id": "", "last_released_at": now.UnixMilli()}).Error; err != nil {
		return err
	}
	return tx.Model(&mdb.Orders{}).Where("trade_id = ? AND collection_mode = ? AND released_at_ms = 0", tradeID, mdb.CollectionExclusive).Update("released_at_ms", now.UnixMilli()).Error
}

func closeExclusiveTx(tx *gorm.DB, order mdb.Orders, now time.Time, notify bool) error {
	var group []mdb.Orders
	q := tx.Where("trade_id = ?", order.TradeId)
	if order.ParentTradeId == "" {
		q = tx.Where("trade_id = ? OR parent_trade_id = ?", order.TradeId, order.TradeId)
	}
	if err := q.Find(&group).Error; err != nil {
		return err
	}
	reason := mdb.OrderClosedManual
	if notify {
		reason = mdb.OrderClosedExpired
	}
	for _, item := range group {
		if item.CollectionMode != mdb.CollectionExclusive {
			continue
		}
		ack := mdb.CallBackConfirmOk
		if notify && item.ParentTradeId == "" && item.NotifyUrl != "" && item.Status == mdb.StatusWaitPay {
			ack = mdb.CallBackConfirmNo
		}
		if err := tx.Model(&mdb.Orders{}).Where("id = ? AND status IN ?", item.ID, []int{mdb.StatusWaitPay, mdb.StatusWaitSelect}).Updates(map[string]interface{}{
			"status": mdb.StatusExpired, "closed_reason": reason, "callback_confirm": ack, "callback_num": 0,
		}).Error; err != nil {
			return err
		}
		if item.PayProvider != "" && item.PayProvider != mdb.PaymentProviderOnChain {
			if err := tx.Model(&mdb.ProviderOrder{}).Where("trade_id = ? AND provider = ? AND status IN ?", item.TradeId, item.PayProvider, []string{mdb.ProviderOrderStatusCreating, mdb.ProviderOrderStatusPending, mdb.ProviderOrderStatusFailed}).Update("status", mdb.ProviderOrderStatusExpired).Error; err != nil {
				return err
			}
		}
		if err := releaseExclusiveTx(tx, item.TradeId, now); err != nil {
			return err
		}
	}
	return nil
}

func expireExclusiveTx(tx *gorm.DB, now time.Time) error {
	var orders []mdb.Orders
	if err := tx.Where("collection_mode = ? AND status IN ? AND payment_expires_at <= ?", mdb.CollectionExclusive, []int{mdb.StatusWaitPay, mdb.StatusWaitSelect}, now).Find(&orders).Error; err != nil {
		return err
	}
	for _, order := range orders {
		if err := closeExclusiveTx(tx, order, now, true); err != nil {
			return err
		}
	}
	return nil
}

func ExpireExclusivePayments() error {
	CollectionMu.Lock()
	defer CollectionMu.Unlock()
	return dao.Mdb.Transaction(func(tx *gorm.DB) error { return expireExclusiveTx(tx, time.Now()) })
}

func CloseExclusivePayment(tradeID string) error {
	CollectionMu.Lock()
	defer CollectionMu.Unlock()
	return dao.Mdb.Transaction(func(tx *gorm.DB) error {
		var order mdb.Orders
		if err := tx.Where("trade_id = ?", tradeID).First(&order).Error; err != nil {
			return err
		}
		if order.Status != mdb.StatusWaitPay && order.Status != mdb.StatusWaitSelect {
			return constant.OrderStatusConflict
		}
		return closeExclusiveTx(tx, order, time.Now(), false)
	})
}

type ObservedPayment struct {
	Network, Address, Token, AssetID, TxHash, EventID string
	RawAmount                                         *big.Int
	Decimals                                          int
	BlockTimeMs                                       int64
}

// ProcessExclusivePayment returns handled=true for an exclusive assignment or
// its history, including exceptions. Callers must not fall back to amount/URL
// matching once handled, even if no order was settled.
func ProcessExclusivePayment(p ObservedPayment) (bool, error) {
	handled, _, err := ProcessExclusivePaymentWithResult(p)
	return handled, err
}

func ProcessExclusivePaymentWithResult(p ObservedPayment) (handled bool, settled *mdb.Orders, err error) {
	if p.RawAmount == nil || p.RawAmount.Sign() <= 0 || p.Decimals < 0 || p.Decimals > 36 || p.TxHash == "" || p.EventID == "" {
		return false, nil, nil
	}
	p.Network = normalizeLockNetwork(p.Network)
	p.TxHash = CanonicalPaymentTransactionID(p.Network, p.TxHash)
	p.Address = normalizeLockAddress(p.Network, p.Address)
	p.Token = normalizeLockToken(p.Token)
	p.AssetID = PaymentAssetID(p.Network, p.AssetID)
	CollectionMu.Lock()
	defer CollectionMu.Unlock()
	err = dao.Mdb.Transaction(func(tx *gorm.DB) error {
		var slots int64
		if err := tx.Model(&mdb.PaymentAddress{}).Where("network = ? AND address = ?", p.Network, p.Address).Count(&slots).Error; err != nil {
			return err
		}
		if slots == 0 {
			return nil
		}
		// Another asset can still have a legacy order at this wallet during
		// migration. Do not consume its event into the exclusive exception inbox.
		var sameAsset int64
		if err := tx.Model(&mdb.PaymentAddress{}).Where("network = ? AND address = ? AND (asset_id = ? OR token = ?)", p.Network, p.Address, p.AssetID, p.Token).Count(&sameAsset).Error; err != nil {
			return err
		}
		if sameAsset == 0 && p.Token != "" {
			var legacy int64
			legacyQuery := tx.Model(&mdb.Orders{}).Where("network = ? AND token = ? AND collection_mode <> ? AND status IN ?", p.Network, p.Token, mdb.CollectionExclusive, []int{mdb.StatusWaitPay, mdb.StatusExpired})
			if err := applyOrderReceiveAddressFilter(legacyQuery, p.Network, p.Address).Count(&legacy).Error; err != nil {
				return err
			}
			if legacy > 0 {
				return nil
			}
		}
		handled = true
		key := paymentEventKey(p.Network, p.TxHash, p.EventID)
		var existing mdb.PaymentReceipt
		if err := tx.Where("event_key = ?", key).Find(&existing).Error; err != nil {
			return err
		}
		if existing.ID != 0 && (existing.Status != "exception" || existing.ResolvedAt != nil ||
			(existing.Reason != "closed_order" && existing.Reason != "assignment_released" && existing.Reason != "parent_closed")) {
			return nil
		}
		if existing.ID != 0 && (existing.RawAmount != p.RawAmount.String() || existing.BlockTimeMs != p.BlockTimeMs ||
			existing.AssetID != p.AssetID || existing.Decimals != p.Decimals || existing.Address != p.Address) {
			return fmt.Errorf("replayed payment evidence differs from historical receipt")
		}
		now := time.Now()
		if err := expireExclusiveTx(tx, now); err != nil {
			return err
		}
		amount := decimal.NewFromBigInt(p.RawAmount, -int32(p.Decimals))
		receipt := mdb.PaymentReceipt{EventKey: key, Network: p.Network, Address: p.Address, Token: p.Token, AssetID: p.AssetID,
			TxHash: p.TxHash, EventID: p.EventID, RawAmount: p.RawAmount.String(), Decimals: p.Decimals, Amount: amount.String(),
			BlockTimeMs: p.BlockTimeMs, Status: "exception", Reason: "unassigned"}
		receipt.ID = existing.ID
		persist := func() error {
			if receipt.ID != 0 {
				return tx.Model(&existing).Updates(map[string]interface{}{"status": receipt.Status, "reason": receipt.Reason,
					"trade_id": receipt.TradeID, "parent_trade_id": receipt.ParentTradeID}).Error
			}
			return tx.Create(&receipt).Error
		}
		if p.BlockTimeMs <= 0 {
			receipt.Reason = "missing_chain_time"
			return persist()
		}
		chainTimeStart, chainTimeEnd := p.BlockTimeMs, p.BlockTimeMs
		if p.Network == mdb.NetworkSolana || p.Network == mdb.NetworkTon || isEVMNetwork(p.Network) {
			// These scanners only receive whole-second chain timestamps. The
			// entire possible interval must belong to the same assignment.
			chainTimeStart = p.BlockTimeMs / 1000 * 1000
			chainTimeEnd = chainTimeStart + 999
		}
		var candidate mdb.Orders
		// For millisecond timestamps, exclude every assignment after payment.
		// For second timestamps, inspect the latest possible assignment, but
		// never automatically choose it when allocation happened mid-interval.
		lookup := tx.Where("collection_mode = ? AND network = ? AND receive_address = ? AND token = ? AND assigned_at_ms > 0 AND assigned_at_ms <= ?",
			mdb.CollectionExclusive, p.Network, p.Address, p.Token, chainTimeEnd).
			Order("assigned_at_ms DESC, id DESC").First(&candidate)
		if errors.Is(lookup.Error, gorm.ErrRecordNotFound) {
			receipt.Reason = "asset_or_assignment_mismatch"
			return persist()
		}
		if lookup.Error != nil {
			return lookup.Error
		}
		if candidate.AssignedAtMs > chainTimeStart {
			// Keep the receipt unassigned: it may predate this order or belong
			// to a previous owner of the address. Do not fall back to that owner.
			receipt.Reason = "ambiguous_chain_time"
			return persist()
		}
		order := &candidate
		receipt.TradeID, receipt.ParentTradeID = order.TradeId, order.ParentTradeId
		if p.AssetID != order.AssetID || p.Decimals != order.AssetDecimals {
			receipt.Reason = "asset_mismatch"
			return persist()
		}
		minimum, _ := decimal.NewFromString(order.MinimumPayment)
		if amount.LessThan(minimum) {
			receipt.Reason = "below_minimum"
			return persist()
		}
		if order.PaymentExpiresAt == nil || chainTimeStart >= order.PaymentExpiresAt.UnixMilli() {
			receipt.Reason = "late_payment"
			return persist()
		}
		if chainTimeEnd >= order.PaymentExpiresAt.UnixMilli() {
			receipt.Reason = "ambiguous_chain_time"
			return persist()
		}
		if !canSettleObservedOrder(order) {
			receipt.Reason = "duplicate_payment"
			if order.Status == mdb.StatusExpired {
				receipt.Reason = "closed_order"
			}
			return persist()
		}
		var slot mdb.PaymentAddress
		if err := tx.Where("network = ? AND address = ? AND asset_id = ?", p.Network, p.Address, p.AssetID).First(&slot).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if order.Status == mdb.StatusWaitPay && slot.TradeID != order.TradeId {
			receipt.Reason = "assignment_released"
			return persist()
		}
		root := order
		if order.ParentTradeId != "" {
			root = &mdb.Orders{}
			if err := tx.Where("trade_id = ?", order.ParentTradeId).First(root).Error; err != nil {
				return err
			}
			if !canSettleObservedOrder(root) {
				receipt.Reason = "parent_closed"
				return persist()
			}
		}
		rate, err := decimal.NewFromString(order.FiatPerToken)
		if err != nil || !rate.IsPositive() {
			return fmt.Errorf("invalid settlement rate for %s", order.TradeId)
		}
		paid := amount.Mul(rate).Round(2)
		if !paid.IsPositive() {
			receipt.Reason = "below_settlement_precision"
			return persist()
		}
		if existing.ID == 0 {
			claimed, err := PaymentEventAlreadyClaimed(tx, p.Network, p.TxHash, p.EventID)
			if err != nil {
				return err
			}
			if claimed {
				receipt.Reason = "duplicate_payment"
				return persist()
			}
		}
		if err := settleObservedPaymentTx(tx, order, root, paid, amount.String(), p.TxHash, now); err != nil {
			return err
		}
		receipt.Status, receipt.Reason = "settled", ""
		settled = &mdb.Orders{}
		if err := tx.Where("id = ?", order.ID).First(settled).Error; err != nil {
			return err
		}
		return persist()
	})
	if err != nil {
		settled = nil
	}
	return
}

// settleExclusiveTx atomically selects the winning channel, closes siblings,
// releases all owned addresses and queues the root callback via its DB state.
func settleExclusiveTx(tx *gorm.DB, order, root *mdb.Orders, paid decimal.Decimal, cryptoAmount, txHash string, now time.Time) error {
	if order.Status != mdb.StatusWaitPay || root.Status != mdb.StatusWaitPay {
		return constant.OrderStatusConflict
	}
	return settleObservedPaymentTx(tx, order, root, paid, cryptoAmount, txHash, now)
}

// The caller has verified chain time and historical ownership. Compare both
// observed statuses so a concurrent expiry/payment cannot silently win twice.
func settleObservedPaymentTx(tx *gorm.DB, order, root *mdb.Orders, paid decimal.Decimal, cryptoAmount, txHash string, now time.Time) error {
	paidFloat, _ := paid.Float64()
	updates := map[string]interface{}{"status": mdb.StatusPaySuccess, "paid_amount": paidFloat, "paid_crypto_amount": cryptoAmount,
		"block_transaction_id": txHash, "callback_confirm": mdb.CallBackConfirmNo, "callback_num": 0, "released_at_ms": now.UnixMilli(), "closed_reason": ""}
	if root.TradeId != order.TradeId {
		updates["callback_confirm"] = mdb.CallBackConfirmOk
	}
	result := tx.Model(&mdb.Orders{}).Where("id = ? AND status = ?", order.ID, order.Status).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return constant.OrderStatusConflict
	}
	if root.TradeId != order.TradeId {
		result := tx.Model(&mdb.Orders{}).Where("id = ? AND status = ?", root.ID, root.Status).Updates(map[string]interface{}{
			"status": mdb.StatusPaySuccess, "paid_amount": paidFloat, "paid_crypto_amount": cryptoAmount, "callback_confirm": mdb.CallBackConfirmNo,
			"callback_num": 0, "pay_by_sub_id": order.ID, "released_at_ms": now.UnixMilli(), "closed_reason": "",
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return constant.OrderStatusConflict
		}
	}
	var group []mdb.Orders
	if err := tx.Where("trade_id = ? OR parent_trade_id = ?", root.TradeId, root.TradeId).Find(&group).Error; err != nil {
		return err
	}
	for _, item := range group {
		if item.Status == mdb.StatusWaitPay || item.Status == mdb.StatusWaitSelect {
			if err := tx.Model(&mdb.Orders{}).Where("id = ?", item.ID).Updates(map[string]interface{}{"status": mdb.StatusExpired, "closed_reason": mdb.OrderClosedSuperseded, "callback_confirm": mdb.CallBackConfirmOk, "released_at_ms": now.UnixMilli()}).Error; err != nil {
				return err
			}
		}
		if item.TradeId != order.TradeId && item.PayProvider != "" && item.PayProvider != mdb.PaymentProviderOnChain {
			if err := tx.Model(&mdb.ProviderOrder{}).Where("trade_id = ? AND provider = ? AND status IN ?", item.TradeId, item.PayProvider, []string{mdb.ProviderOrderStatusCreating, mdb.ProviderOrderStatusPending, mdb.ProviderOrderStatusFailed}).Update("status", mdb.ProviderOrderStatusExpired).Error; err != nil {
				return err
			}
		}
		if err := releaseExclusiveTx(tx, item.TradeId, now); err != nil {
			return err
		}
	}
	return nil
}

// SettleExclusiveProvider is only for an authenticated hosted-provider callback.
// On-chain payments must go through the observed-receipt path instead.
func SettleExclusiveProvider(tradeID, txHash string) error {
	CollectionMu.Lock()
	defer CollectionMu.Unlock()
	return dao.Mdb.Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		if err := expireExclusiveTx(tx, now); err != nil {
			return err
		}
		var order mdb.Orders
		if err := tx.Where("trade_id = ?", tradeID).First(&order).Error; err != nil {
			return err
		}
		if order.PayProvider != mdb.PaymentProviderOkPay {
			return constant.ManualPaymentVerifyErr
		}
		if order.Status == mdb.StatusPaySuccess && order.BlockTransactionId == txHash {
			return constant.OrderBlockAlreadyProcess
		}
		if order.Status != mdb.StatusWaitPay {
			return constant.OrderStatusConflict
		}
		root := &order
		if order.ParentTradeId != "" {
			root = &mdb.Orders{}
			if err := tx.Where("trade_id = ?", order.ParentTradeId).First(root).Error; err != nil {
				return err
			}
			if root.Status != mdb.StatusWaitPay {
				return constant.OrderStatusConflict
			}
		}
		return settleExclusiveTx(tx, &order, root, decimal.NewFromFloat(order.Amount), decimal.NewFromFloat(order.ActualAmount).String(), txHash, now)
	})
}

func LoadPaymentCursor(key string) (string, error) {
	var row mdb.PaymentScanCursor
	err := dao.Mdb.Where("key = ?", key).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", nil
	}
	return row.Position, err
}
func SavePaymentCursor(key, position string) error {
	return dao.Mdb.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "key"}}, DoUpdates: clause.AssignmentColumns([]string{"position", "updated_at"})}).Create(&mdb.PaymentScanCursor{Key: key, Position: position}).Error
}
