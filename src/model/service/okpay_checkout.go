package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/response"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// SwitchNetwork serializes checkout attempts with gCreateTransactionLock. Only
// short DB transitions hold CollectionMu; HTTP must allow scanner/notify wins.
func resumeOkPayCheckout(tradeID string) (*response.CheckoutCounterResponse, error) {
	var order mdb.Orders
	var provider mdb.ProviderOrder
	data.CollectionMu.Lock()
	err := dao.Mdb.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("trade_id = ?", tradeID).First(&order).Error; err != nil {
			return err
		}
		if err := tx.Where("trade_id = ? AND provider = ?", tradeID, mdb.PaymentProviderOkPay).First(&provider).Error; err != nil {
			return err
		}
		if order.PayProvider != mdb.PaymentProviderOkPay || provider.Amount != order.ActualAmount || !strings.EqualFold(provider.Coin, order.Token) {
			return constant.OrderStatusConflict
		}
		if !data.PaymentDeadline(&order).After(time.Now()) {
			return constant.OrderStatusConflict
		}
		if order.ParentTradeId != "" {
			var parent mdb.Orders
			if err := tx.Where("trade_id = ?", order.ParentTradeId).First(&parent).Error; err != nil {
				return err
			}
			if parent.Status != mdb.StatusWaitPay || !data.PaymentDeadline(&parent).After(time.Now()) {
				return constant.OrderStatusConflict
			}
			if err := tx.Model(&parent).Update("is_selected", true).Error; err != nil {
				return err
			}
		}
		// Compatibility for failed attempts retired by the previous version.
		// Never revive manual closures, natural expiry or another channel's win.
		if order.ParentTradeId != "" && order.Status == mdb.StatusExpired && order.ClosedReason == mdb.OrderClosedSuperseded && provider.Status == mdb.ProviderOrderStatusFailed && provider.PayURL == "" && order.BlockTransactionId == "" && order.PayBySubId == 0 {
			if err := tx.Model(&order).Updates(map[string]interface{}{"status": mdb.StatusWaitPay, "closed_reason": "", "released_at_ms": 0}).Error; err != nil {
				return err
			}
			order.Status = mdb.StatusWaitPay
		}
		if order.Status != mdb.StatusWaitPay {
			return constant.OrderStatusConflict
		}
		if order.ParentTradeId != "" && !order.IsSelected {
			if err := tx.Model(&order).Update("is_selected", true).Error; err != nil {
				return err
			}
			order.IsSelected = true
		}
		switch provider.Status {
		case mdb.ProviderOrderStatusCreating, mdb.ProviderOrderStatusFailed, mdb.ProviderOrderStatusPending:
		default:
			return constant.OrderStatusConflict
		}
		if provider.RequestAmount == "" {
			// Amount was rounded when the attempt was reserved. A configuration
			// change before restart must not round that durable quote a second time.
			provider.RequestAmount = decimal.NewFromFloat(provider.Amount).String()
			provider.ReturnURL = strings.TrimSpace(order.RedirectUrl)
			if provider.ReturnURL == "" {
				provider.ReturnURL = data.GetOkPayReturnURL()
			}
			if provider.ReturnURL == "" {
				provider.ReturnURL = fmt.Sprintf("%s/pay/checkout-counter/%s", config.GetAppUri(), tradeID)
			}
			if err := tx.Model(&provider).Updates(map[string]interface{}{"request_amount": provider.RequestAmount, "return_url": provider.ReturnURL}).Error; err != nil {
				return err
			}
		}
		if provider.PayURL != "" {
			return nil
		}
		return tx.Model(&provider).Update("status", mdb.ProviderOrderStatusCreating).Error
	})
	data.CollectionMu.Unlock()
	if err != nil {
		return nil, err
	}
	if provider.PayURL != "" {
		view := buildCheckoutResponse(&order)
		view.PaymentUrl = provider.PayURL
		return view, nil
	}

	created, err := createOkPayDepositRequest(tradeID, provider.RequestAmount, provider.Coin, provider.ReturnURL)
	if err != nil {
		// The remote outcome may be unknown. Preserve the local payment target,
		// quote and unique_id; an early successful callback must not be downgraded.
		if saveErr := data.MarkProviderOrderFailed(tradeID, mdb.PaymentProviderOkPay); saveErr != nil {
			return nil, saveErr
		}
		return nil, constant.PaymentProviderCreateErr
	}
	data.CollectionMu.Lock()
	err = dao.Mdb.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("trade_id = ?", tradeID).First(&order).Error; err != nil {
			return err
		}
		if order.Status != mdb.StatusPaySuccess && (order.Status != mdb.StatusWaitPay || !data.PaymentDeadline(&order).After(time.Now())) {
			return constant.OrderStatusConflict
		}
		if order.Status == mdb.StatusPaySuccess && order.BlockTransactionId != created.ProviderOrderID {
			return constant.OrderStatusConflict
		}
		if order.ParentTradeId != "" {
			var parent mdb.Orders
			if err := tx.Where("trade_id = ?", order.ParentTradeId).First(&parent).Error; err != nil {
				return err
			}
			winning := parent.Status == mdb.StatusPaySuccess && parent.PayBySubId == order.ID && order.Status == mdb.StatusPaySuccess
			if !winning && (parent.Status != mdb.StatusWaitPay || !data.PaymentDeadline(&parent).After(time.Now())) {
				return constant.OrderStatusConflict
			}
		}
		return data.UpdateProviderOrderCreatedWithTransaction(tx, tradeID, mdb.PaymentProviderOkPay, created.ProviderOrderID, created.PayURL)
	})
	data.CollectionMu.Unlock()
	if err != nil {
		return nil, err
	}
	view := buildCheckoutResponse(&order)
	view.PaymentUrl = created.PayURL
	return view, nil
}
