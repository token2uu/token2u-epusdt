package service

import (
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/response"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/GMWalletApp/epusdt/util/log"
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
	key := normalizeOrderAddressByNetwork(network, address)
	if network == mdb.NetworkTon {
		var err error
		key, err = addressutil.TonRawAddressKey(address)
		if err != nil {
			return false, err
		}
	}
	var count int64
	err := dao.Mdb.Model(&mdb.PaymentAddress{}).Where("network = ? AND address = ? AND token = ?", network, key, token).Count(&count).Error
	return count > 0, err
}

// The durable receipt decides whether this is a new settlement; replayed events
// never emit another operator notification.
func processObservedPayment(p data.ObservedPayment) (bool, error) {
	handled, settled, err := data.SettleLegacyObservedPaymentWithResult(p)
	if err == nil && settled != nil {
		root := settled.TradeId
		if settled.ParentTradeId != "" {
			root = settled.ParentTradeId
		}
		group, loadErr := data.GetAllSubOrders(root)
		if loadErr != nil {
			log.Sugar.Warnf("[PAYMENT-RECOVERY] load reservations trade_id=%s: %v", root, loadErr)
		}
		group = append(group, *settled, mdb.Orders{TradeId: root})
		for _, order := range group {
			if unlockErr := data.UnLockTransactionByTradeId(order.TradeId); unlockErr != nil {
				// Payment and callback state are already durable. Stale runtime
				// reservations also have their own expiry/cleanup worker.
				log.Sugar.Warnf("[PAYMENT-RECOVERY] release reservation trade_id=%s: %v", order.TradeId, unlockErr)
			}
		}
	}
	if err == nil && !handled {
		handled, settled, err = data.ProcessExclusivePaymentWithResult(p)
	}
	if err == nil && settled != nil {
		sendPaymentNotification(settled)
	}
	return handled, err
}
