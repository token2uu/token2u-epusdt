package service

import (
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/response"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/shopspring/decimal"
)

func setNewCollectionMode(order *mdb.Orders) {
	if strings.EqualFold(order.PaymentType, mdb.PaymentTypeEpay) && config.EpayExclusiveAddresses() {
		order.CollectionMode = mdb.CollectionExclusive
		expires := time.Now().Add(config.GetOrderExpirationTimeDuration())
		order.PaymentExpiresAt = &expires
	}
}

func orderExpiryMillis(order *mdb.Orders) int64 {
	if order.PaymentExpiresAt != nil {
		return order.PaymentExpiresAt.UnixMilli()
	}
	return order.CreatedAt.AddMinutes(config.GetOrderExpirationTime()).TimestampMilli()
}

func switchExclusiveNetwork(parent *mdb.Orders, token, network string) (*response.CheckoutCounterResponse, error) {
	if err := data.ExpireExclusivePayments(); err != nil {
		return nil, err
	}
	fresh, err := data.GetOrderInfoByTradeId(parent.TradeId)
	if err != nil {
		return nil, err
	}
	parent = fresh
	if parent.Status != mdb.StatusWaitPay && parent.Status != mdb.StatusWaitSelect {
		return nil, constant.OrderNotWaitPay
	}
	if parent.Token == token && parent.Network == network {
		selected, err := data.SelectExclusivePayment(parent.TradeId)
		if err != nil {
			return nil, err
		}
		return buildCheckoutResponse(selected), nil
	}
	if !data.IsChainEnabled(network) {
		return nil, constant.ChainNotEnabled
	}
	asset, err := ensureEnabledOrderAsset(network, token)
	if err != nil {
		return nil, err
	}
	if parent.Status != mdb.StatusWaitSelect {
		existing, err := data.GetSubOrderByTokenNetwork(parent.TradeId, token, network)
		if err != nil {
			return nil, err
		}
		if existing.ID > 0 {
			selected, err := data.SelectExclusivePayment(existing.TradeId)
			if err != nil {
				return nil, err
			}
			return buildCheckoutResponse(selected), nil
		}
		count, err := data.CountSubOrders(parent.TradeId)
		if err != nil {
			return nil, err
		}
		if count >= MaxSubOrders {
			return nil, constant.SubOrderLimitExceeded
		}
	}
	rate := config.GetRateForCoin(strings.ToLower(token), strings.ToLower(parent.Currency))
	if rate <= 0 {
		return nil, constant.RateAmountErr
	}
	amount := decimal.NewFromFloat(parent.Amount).Mul(decimal.NewFromFloat(rate)).Round(int32(data.GetAmountPrecision()))
	if amount.LessThan(decimal.NewFromFloat(UsdtMinimumPaymentAmount)) {
		return nil, constant.PayAmountErr
	}
	wallets, err := data.GetAvailableWalletAddressByNetwork(network)
	if err != nil {
		return nil, err
	}
	if len(wallets) == 0 {
		return nil, constant.NotAvailableWalletAddress
	}
	order := *parent
	placeholder := parent.Status == mdb.StatusWaitSelect
	if !placeholder {
		order.BaseModel = mdb.BaseModel{}
		order.TradeId = GenerateCode()
		order.OrderId = order.TradeId
		order.ParentTradeId = parent.TradeId
		order.NotifyUrl = ""
		order.CallBackConfirm = mdb.CallBackConfirmOk
	}
	order.Token, order.Network = token, network
	order.ActualAmount, _ = amount.Float64()
	order.Status, order.IsSelected = mdb.StatusWaitPay, true
	order.PayProvider = mdb.PaymentProviderOnChain
	if err := data.CreateExclusivePayment(&order, *asset, wallets, placeholder); err != nil {
		return nil, err
	}
	return buildCheckoutResponse(&order), nil
}

// Legacy and exclusive reservations must never overlap during a rolling
// migration. Retired exclusive slots remain reserved from the legacy allocator;
// new exclusive orders may reuse them through the ownership-aware allocator.
func addressHasExclusiveHistory(network, address, token string) (bool, error) {
	var count int64
	err := dao.Mdb.Model(&mdb.PaymentAddress{}).Where("network = ? AND address = ? AND token = ?", network, normalizeOrderAddressByNetwork(network, address), token).Count(&count).Error
	return count > 0, err
}

// The durable receipt decides whether this is a new settlement; replayed events
// never emit another operator notification.
func processExclusivePayment(p data.ObservedPayment) (bool, error) {
	handled, settled, err := data.ProcessExclusivePaymentWithResult(p)
	if err == nil && settled != nil {
		sendPaymentNotification(settled)
	}
	return handled, err
}
