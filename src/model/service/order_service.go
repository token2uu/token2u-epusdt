package service

import (
	cryptorand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/model/response"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/GMWalletApp/epusdt/util/math"
	"github.com/GMWalletApp/epusdt/util/security"
	"github.com/dromara/carbon/v2"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

const (
	CnyMinimumPaymentAmount  = 0.01
	UsdtMinimumPaymentAmount = 0.01
	IncrementalMaximumNumber = 100
)

var (
	gCreateTransactionLock sync.Mutex
	gOrderProcessingLock   sync.Mutex
)

var (
	manualPaymentWaitPayStatuses = []int{mdb.StatusWaitPay}
	adminManualPaymentStatuses   = []int{mdb.StatusWaitPay, mdb.StatusExpired}
)

type orderProcessingOptions struct {
	allowedStatuses       []int
	parentAllowedStatuses []int
	authenticatedProvider bool
}

// apiKeyID safely extracts the primary key from an ApiKey row.
// Returns 0 when apiKey is nil (middleware didn't run — shouldn't happen on authed routes).
func apiKeyID(apiKey *mdb.ApiKey) uint64 {
	if apiKey == nil {
		return 0
	}
	return apiKey.ID
}

func normalizeOrderAddressByNetwork(network, address string) string {
	network = strings.ToLower(strings.TrimSpace(network))
	address = strings.TrimSpace(address)
	switch network {
	case mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma:
		return strings.ToLower(address)
	case mdb.NetworkTon:
		if normalized, err := addressutil.NormalizeTonAddress(address); err == nil {
			return normalized
		}
		return address
	case mdb.NetworkAptos:
		if normalized, err := addressutil.NormalizeMoveAddress(address); err == nil {
			return normalized
		}
		return address
	default:
		return address
	}
}

func ensureEnabledOrderAsset(network, token string) (*mdb.ChainToken, error) {
	tokenRow, err := data.GetEnabledChainTokenBySymbol(network, token)
	if err != nil {
		return nil, err
	}
	if tokenRow == nil || tokenRow.ID == 0 {
		return nil, constant.SupportedAssetNotFound
	}
	if !ChainTokenReadyForPayment(*tokenRow) {
		return nil, constant.SupportedAssetNotFound
	}
	return tokenRow, nil
}

func validateCreateTransactionTokenNetwork(token, network string) error {
	if (token == "") != (network == "") {
		return constant.ParamsMarshalErr
	}
	return nil
}

func buildCreateTransactionResponse(order *mdb.Orders) *response.CreateTransactionResponse {
	expiry := carbon.Now().AddMinutes(config.GetOrderExpirationTime()).Timestamp()
	if order.PaymentExpiresAt != nil {
		expiry = order.PaymentExpiresAt.Unix()
	}
	return &response.CreateTransactionResponse{
		TradeId:        order.TradeId,
		OrderId:        order.OrderId,
		Amount:         order.Amount,
		Currency:       order.Currency,
		ActualAmount:   order.ActualAmount,
		ReceiveAddress: order.ReceiveAddress,
		Token:          order.Token,
		Status:         order.Status,
		ExpirationTime: expiry,
		PaymentUrl:     fmt.Sprintf("%s/pay/checkout-counter/%s", config.GetAppUri(), order.TradeId),
	}
}

// CreateTransaction creates a new payment order.
func CreateTransaction(req *request.CreateTransactionRequest, apiKey *mdb.ApiKey) (*response.CreateTransactionResponse, error) {
	token := strings.ToUpper(strings.TrimSpace(req.Token))
	currency := strings.ToUpper(strings.TrimSpace(req.Currency))
	network := strings.ToLower(strings.TrimSpace(req.Network))
	if err := validateCreateTransactionTokenNetwork(token, network); err != nil {
		return nil, err
	}
	notifyURL := strings.TrimSpace(req.NotifyUrl)
	if err := security.ValidatePublicHTTPURL(notifyURL); err != nil {
		return nil, constant.NotifyURLErr
	}
	paymentType := mdb.PaymentTypeGmpay
	if strings.EqualFold(req.PaymentType, mdb.PaymentTypeEpay) {
		paymentType = mdb.PaymentTypeEpay
	}
	epayType := ""
	if paymentType == mdb.PaymentTypeEpay {
		epayType = strings.TrimSpace(req.EpayType)
	}

	gCreateTransactionLock.Lock()
	defer gCreateTransactionLock.Unlock()

	amountPrecision := data.GetAmountPrecision()
	payAmount := math.MustParsePrecFloat64(req.Amount, amountPrecision)
	exist, err := data.GetOrderInfoByOrderId(req.OrderId)
	if err != nil {
		return nil, err
	}
	if exist.ID > 0 {
		return nil, constant.OrderAlreadyExists
	}

	decimalPayAmount := decimal.NewFromFloat(payAmount)
	if decimalPayAmount.Cmp(decimal.NewFromFloat(CnyMinimumPaymentAmount)) == -1 {
		return nil, constant.PayAmountErr
	}

	if token == "" && network == "" {
		tradeID := GenerateCode()
		order := &mdb.Orders{
			TradeId:     tradeID,
			OrderId:     req.OrderId,
			Amount:      payAmount,
			Currency:    currency,
			Status:      mdb.StatusWaitSelect,
			NotifyUrl:   notifyURL,
			RedirectUrl: req.RedirectUrl,
			Name:        req.Name,
			EpayType:    epayType,
			PaymentType: paymentType,
			PayProvider: mdb.PaymentProviderOnChain,
			ApiKeyID:    apiKeyID(apiKey),
		}
		setNewCollectionMode(order)
		if err = data.CreateOrderWithTransaction(dao.Mdb, order); err != nil {
			return nil, err
		}
		return buildCreateTransactionResponse(order), nil
	}

	if !data.IsChainEnabled(network) {
		return nil, constant.ChainNotEnabled
	}
	if _, err = ensureEnabledOrderAsset(network, token); err != nil {
		return nil, err
	}

	rate := config.GetRateForCoin(strings.ToLower(token), strings.ToLower(currency))
	if rate <= 0 {
		return nil, constant.RateAmountErr
	}

	decimalTokenAmount := decimalPayAmount.Mul(decimal.NewFromFloat(rate))
	if decimalTokenAmount.Cmp(decimal.NewFromFloat(UsdtMinimumPaymentAmount)) == -1 {
		return nil, constant.PayAmountErr
	}

	walletAddress, err := data.GetAvailableWalletAddressByNetwork(network)
	if err != nil {
		return nil, err
	}
	if len(walletAddress) <= 0 {
		return nil, constant.NotAvailableWalletAddress
	}

	tradeID := GenerateCode()
	amount := math.MustParsePrecFloat64(decimalTokenAmount.InexactFloat64(), amountPrecision)
	if paymentType == mdb.PaymentTypeEpay && config.EpayExclusiveAddresses() {
		asset, assetErr := ensureEnabledOrderAsset(network, token)
		if assetErr != nil {
			return nil, assetErr
		}
		order := &mdb.Orders{TradeId: tradeID, OrderId: req.OrderId, Amount: payAmount, Currency: currency,
			ActualAmount: amount, Token: token, Network: network, Status: mdb.StatusWaitPay, NotifyUrl: notifyURL,
			RedirectUrl: req.RedirectUrl, Name: req.Name, EpayType: epayType, PaymentType: paymentType,
			PayProvider: mdb.PaymentProviderOnChain, ApiKeyID: apiKeyID(apiKey)}
		setNewCollectionMode(order)
		if err := data.CreateExclusivePayment(order, *asset, walletAddress, false); err != nil {
			return nil, err
		}
		return buildCreateTransactionResponse(order), nil
	}
	availableAddress, availableAmount, err := ReserveAvailableWalletAndAmount(tradeID, network, token, amount, walletAddress)
	if err != nil {
		return nil, err
	}
	if availableAddress == "" {
		return nil, constant.NotAvailableAmountErr
	}

	tx := dao.Mdb.Begin()
	order := &mdb.Orders{
		TradeId:        tradeID,
		OrderId:        req.OrderId,
		Amount:         payAmount,
		Currency:       currency,
		ActualAmount:   availableAmount,
		ReceiveAddress: availableAddress,
		Token:          token,
		Network:        network,
		Status:         mdb.StatusWaitPay,
		NotifyUrl:      notifyURL,
		RedirectUrl:    req.RedirectUrl,
		Name:           req.Name,
		EpayType:       epayType,
		PaymentType:    paymentType,
		PayProvider:    mdb.PaymentProviderOnChain,
		ApiKeyID:       apiKeyID(apiKey),
	}
	if err = data.CreateOrderWithTransaction(tx, order); err != nil {
		tx.Rollback()
		_ = data.UnLockTransactionByTradeId(tradeID)
		return nil, err
	}
	if err = tx.Commit().Error; err != nil {
		tx.Rollback()
		_ = data.UnLockTransactionByTradeId(tradeID)
		return nil, err
	}

	return buildCreateTransactionResponse(order), nil
}

// OrderProcessing marks an order as paid and releases its sqlite reservation.
func OrderProcessing(req *request.OrderProcessingRequest) error {
	return orderProcessing(req, orderProcessingOptions{
		allowedStatuses:       manualPaymentWaitPayStatuses,
		parentAllowedStatuses: manualPaymentWaitPayStatuses,
	})
}

func orderProcessingWithAllowedStatuses(req *request.OrderProcessingRequest, allowedStatuses []int) error {
	return orderProcessing(req, orderProcessingOptions{
		allowedStatuses:       allowedStatuses,
		parentAllowedStatuses: allowedStatuses,
	})
}

func orderProcessing(req *request.OrderProcessingRequest, opts orderProcessingOptions) error {
	gOrderProcessingLock.Lock()
	defer gOrderProcessingLock.Unlock()

	target, loadErr := data.GetOrderInfoByTradeId(req.TradeId)
	if loadErr != nil {
		return loadErr
	}
	if target.CollectionMode == mdb.CollectionExclusive {
		if target.PayProvider == mdb.PaymentProviderOkPay {
			return data.SettleExclusiveProvider(req.TradeId, req.BlockTransactionId)
		}
		return constant.ManualPaymentVerifyErr
	}

	data.CollectionMu.Lock()
	defer data.CollectionMu.Unlock()
	var group []mdb.Orders
	err := dao.Mdb.Transaction(func(tx *gorm.DB) error {
		var err error
		group, err = data.SettleOrderGroupWithTransaction(tx, req, opts.allowedStatuses, opts.parentAllowedStatuses, opts.authenticatedProvider)
		return err
	})
	if err != nil {
		return err
	}
	// Runtime reservations are separate from the business DB. Settlement and
	// callbacks are durable first; cleanup must never delete a later owner.
	for _, order := range group {
		if err := data.UnLockTransactionByTradeId(order.TradeId); err != nil {
			log.Sugar.Warnf("[order] release paid group reservation failed, trade_id=%s, err=%v", order.TradeId, err)
		}
	}
	return nil
}

// ReserveAvailableWalletAndAmount finds and locks a network+address+token+amount pair.
func ReserveAvailableWalletAndAmount(tradeID string, network string, token string, amount float64, walletAddress []mdb.WalletAddress) (string, float64, error) {
	asset, err := ensureEnabledOrderAsset(network, token)
	if err != nil {
		return "", 0, err
	}
	// Validate the rounded checkout amount before taking a runtime lock.
	// Collision increments must not silently raise an otherwise invalid price.
	if decimal.NewFromFloat(amount).LessThan(decimal.NewFromFloat(asset.MinAmount)) {
		return "", 0, constant.PayAmountErr
	}
	availableAddress := ""
	availableAmount := amount
	amountPrecision := data.GetAmountPrecision()

	tryLockWalletFunc := func(targetAmount float64) (string, error) {
		for _, address := range walletAddress {
			normalizedAddress := normalizeOrderAddressByNetwork(network, address.Address)
			reserved, err := addressHasExclusiveHistory(network, normalizedAddress, token)
			if err != nil {
				return "", err
			}
			if reserved {
				continue
			}
			err = data.LockTransaction(network, normalizedAddress, token, tradeID, targetAmount, config.GetOrderExpirationTimeDuration())
			if err == nil {
				return normalizedAddress, nil
			}
			if errors.Is(err, data.ErrTransactionLocked) {
				continue
			}
			return "", err
		}
		return "", nil
	}

	for i := 0; i < IncrementalMaximumNumber; i++ {
		address, err := tryLockWalletFunc(availableAmount)
		if err != nil {
			return "", 0, err
		}
		if address == "" {
			decimalOldAmount := decimal.NewFromFloat(availableAmount)
			decimalIncr := decimal.New(1, int32(-amountPrecision))
			availableAmount = math.MustParsePrecFloat64(decimalOldAmount.Add(decimalIncr).InexactFloat64(), amountPrecision)
			continue
		}
		availableAddress = address
		break
	}
	return availableAddress, availableAmount, nil
}

// GenerateCode creates a unique trade id.
func GenerateCode() string {
	buf := make([]byte, 18)
	if _, err := cryptorand.Read(buf); err != nil {
		panic(fmt.Sprintf("generate trade id: crypto/rand failed: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

// GetOrderInfoByTradeId returns a validated order.
func GetOrderInfoByTradeId(tradeId string) (*mdb.Orders, error) {
	order, err := data.GetOrderInfoByTradeId(tradeId)
	if err != nil {
		return nil, err
	}
	if order.ID <= 0 {
		return nil, constant.OrderNotExists
	}
	return order, nil
}

// SubmitManualPayment verifies a submitted transaction hash and marks the
// matching admin-selected on-chain order paid. Admin mark-paid may repair
// waiting or expired orders after the submitted chain transaction is verified.
func SubmitManualPayment(tradeId, blockTransactionId string) (*response.ManualPaymentResponse, error) {
	tradeId = strings.TrimSpace(tradeId)
	blockTransactionId = strings.TrimSpace(blockTransactionId)

	order, err := GetOrderInfoByTradeId(tradeId)
	if err != nil {
		return nil, err
	}
	return submitManualPaymentForOrder(order, blockTransactionId, adminManualPaymentStatuses)
}

// SubmitCashierManualPayment is the public cashier variant. Old hash-only
// settlements are rejected before RPC; event-backed transactions are verified
// to distinguish independent transfers. Only pending orders may use this route.
func SubmitCashierManualPayment(tradeId, blockTransactionId string) (*response.ManualPaymentResponse, error) {
	tradeId = strings.TrimSpace(tradeId)
	blockTransactionId = strings.TrimSpace(blockTransactionId)

	order, err := GetOrderInfoByTradeId(tradeId)
	if err != nil {
		return nil, err
	}
	if err = validateManualPaymentOrderWithStatuses(order, manualPaymentWaitPayStatuses); err != nil {
		return nil, err
	}
	// Only old settlements lacking event evidence claim the whole transaction.
	// Known receipts need verification first to resolve the selected transfer.
	data.CollectionMu.Lock()
	claimed, err := data.PaymentTransactionHasUntrackedSettlement(dao.Mdb, order.Network, blockTransactionId)
	data.CollectionMu.Unlock()
	if err != nil {
		return nil, err
	}
	if claimed {
		return nil, constant.OrderBlockAlreadyProcess
	}
	return submitManualPaymentForOrder(order, blockTransactionId, manualPaymentWaitPayStatuses)
}

func submitManualPaymentForOrder(order *mdb.Orders, blockTransactionId string, allowedStatuses []int) (*response.ManualPaymentResponse, error) {
	blockTransactionId = strings.TrimSpace(blockTransactionId)
	if order.CollectionMode == mdb.CollectionExclusive {
		return nil, constant.ManualPaymentVerifyErr
	}
	if err := validateManualPaymentOrderWithStatuses(order, allowedStatuses); err != nil {
		return nil, err
	}

	validationResult, err := ValidateManualOrderPaymentWithPaidAmount(order, blockTransactionId)
	if err != nil {
		var rspErr *constant.RspError
		if errors.As(err, &rspErr) {
			return nil, err
		}
		return nil, constant.ManualPaymentVerifyErr
	}

	req := &request.OrderProcessingRequest{
		ReceiveAddress:     order.ReceiveAddress,
		Currency:           order.Currency,
		Token:              order.Token,
		Network:            order.Network,
		Amount:             order.ActualAmount,
		TradeId:            order.TradeId,
		BlockTransactionId: validationResult.CanonicalTxID,
	}
	if validationResult.Evidence != nil {
		req.ManualPayment = validationResult.Evidence
		req.PaidAmount = validationResult.PaidAmount
	}

	if err = orderProcessingWithAllowedStatuses(req, allowedStatuses); err != nil {
		return nil, err
	}

	updatedOrder, err := GetOrderInfoByTradeId(order.TradeId)
	if err != nil {
		return nil, err
	}
	return &response.ManualPaymentResponse{
		TradeId:            updatedOrder.TradeId,
		Status:             updatedOrder.Status,
		BlockTransactionId: updatedOrder.BlockTransactionId,
	}, nil
}

func validateManualPaymentOrder(order *mdb.Orders) error {
	return validateManualPaymentOrderWithStatuses(order, manualPaymentWaitPayStatuses)
}

func validateManualPaymentOrderWithStatuses(order *mdb.Orders, allowedStatuses []int) error {
	if !manualPaymentStatusAllowed(order.Status, allowedStatuses) {
		return constant.OrderNotWaitPay
	}
	if !isOnChainOrder(order.PayProvider) {
		return constant.ManualPaymentProviderErr
	}
	return nil
}

func manualPaymentStatusAllowed(status int, allowedStatuses []int) bool {
	for _, allowed := range allowedStatuses {
		if status == allowed {
			return true
		}
	}
	return false
}

func isOnChainOrder(payProvider string) bool {
	payProvider = strings.TrimSpace(payProvider)
	return payProvider == "" || payProvider == mdb.PaymentProviderOnChain
}

func hasTransactionLock(order *mdb.Orders) bool {
	return order != nil &&
		isOnChainOrder(order.PayProvider) &&
		strings.TrimSpace(order.Network) != "" &&
		strings.TrimSpace(order.ReceiveAddress) != "" &&
		strings.TrimSpace(order.Token) != "" &&
		order.ActualAmount > 0
}

const MaxSubOrders = 1

// SwitchNetwork completes a status=4 placeholder in place, or creates/returns
// an existing sub-order for a different payment target once the parent is
// already concrete.
func SwitchNetwork(req *request.SwitchNetworkRequest) (*response.CheckoutCounterResponse, error) {
	gCreateTransactionLock.Lock()
	defer gCreateTransactionLock.Unlock()

	token := strings.ToUpper(strings.TrimSpace(req.Token))
	network := strings.ToLower(strings.TrimSpace(req.Network))

	// 1. Load parent order
	parent, err := data.GetOrderInfoByTradeId(req.TradeId)
	if err != nil {
		return nil, err
	}
	if parent.ID <= 0 {
		return nil, constant.OrderNotExists
	}
	if parent.ParentTradeId != "" {
		return nil, constant.CannotSwitchSubOrder
	}
	if parent.Status != mdb.StatusWaitPay && parent.Status != mdb.StatusWaitSelect {
		return nil, constant.OrderNotWaitPay
	}

	if parent.CollectionMode == mdb.CollectionExclusive && network != mdb.PaymentProviderOkPay {
		return switchExclusiveNetwork(parent, token, network)
	}
	if network == mdb.PaymentProviderOkPay {
		return switchToOkPay(parent, token)
	}

	if parent.Status == mdb.StatusWaitSelect {
		return completeWaitSelectOrder(parent, token, network)
	}

	// 2. Same token+network as parent → mark selected and return
	if strings.EqualFold(parent.Token, token) && strings.EqualFold(parent.Network, network) {
		_ = data.MarkOrderSelected(parent.TradeId)
		parent.IsSelected = true
		return buildCheckoutResponse(parent), nil
	}

	// 3. Existing active sub-order for this token+network → return it
	existing, err := data.GetSubOrderByTokenNetwork(parent.TradeId, token, network)
	if err != nil {
		return nil, err
	}
	if existing.ID > 0 {
		_ = data.MarkOrderSelected(parent.TradeId)
		_ = data.MarkOrderSelected(existing.TradeId)
		if err := data.RefreshOrderExpiration(parent.TradeId); err != nil {
			return nil, err
		}
		existing.IsSelected = true
		return buildCheckoutResponse(existing), nil
	}

	// 4. Only one sub-order may ever be created under a parent. Existing
	// active sub-orders are reused above; any other target after that is blocked.
	count, err := data.CountSubOrders(parent.TradeId)
	if err != nil {
		return nil, err
	}
	if count >= MaxSubOrders {
		return nil, constant.SubOrderLimitExceeded
	}

	// 5. Validate the target chain and asset before any rate lookup so
	// disabled/unknown tokens return the stable unsupported-asset error.
	if !data.IsChainEnabled(network) {
		return nil, constant.ChainNotEnabled
	}
	if _, err = ensureEnabledOrderAsset(network, token); err != nil {
		return nil, err
	}

	// 6. Calculate amount for the new network
	rate := config.GetRateForCoin(strings.ToLower(token), strings.ToLower(parent.Currency))
	if rate <= 0 {
		return nil, constant.RateAmountErr
	}
	decimalPayAmount := decimal.NewFromFloat(parent.Amount)
	decimalTokenAmount := decimalPayAmount.Mul(decimal.NewFromFloat(rate))
	if decimalTokenAmount.Cmp(decimal.NewFromFloat(UsdtMinimumPaymentAmount)) == -1 {
		return nil, constant.PayAmountErr
	}

	// 7. Find and lock wallet
	walletAddress, err := data.GetAvailableWalletAddressByNetwork(network)
	if err != nil {
		return nil, err
	}
	if len(walletAddress) <= 0 {
		return nil, constant.NotAvailableWalletAddress
	}

	subTradeID := GenerateCode()
	amount := math.MustParsePrecFloat64(decimalTokenAmount.InexactFloat64(), data.GetAmountPrecision())
	availableAddress, availableAmount, err := ReserveAvailableWalletAndAmount(subTradeID, network, token, amount, walletAddress)
	if err != nil {
		return nil, err
	}
	if availableAddress == "" {
		return nil, constant.NotAvailableAmountErr
	}

	// 8. Create sub-order
	tx := dao.Mdb.Begin()
	subOrder := &mdb.Orders{
		TradeId:         subTradeID,
		OrderId:         subTradeID, // sub-order uses its own trade_id as order_id (unique constraint)
		ParentTradeId:   parent.TradeId,
		Amount:          parent.Amount,
		Currency:        parent.Currency,
		ActualAmount:    availableAmount,
		ReceiveAddress:  availableAddress,
		Token:           token,
		Network:         network,
		Status:          mdb.StatusWaitPay,
		IsSelected:      true,
		NotifyUrl:       "",
		RedirectUrl:     parent.RedirectUrl,
		Name:            parent.Name,
		EpayType:        parent.EpayType,
		CallBackConfirm: mdb.CallBackConfirmOk, // don't trigger callback on sub-order
		PaymentType:     parent.PaymentType,
		PayProvider:     mdb.PaymentProviderOnChain,
		ApiKeyID:        parent.ApiKeyID, // inherit from parent so resolveOrderApiKey never fails
	}
	if err = data.CreateOrderWithTransaction(tx, subOrder); err != nil {
		tx.Rollback()
		_ = data.UnLockTransactionByTradeId(subTradeID)
		return nil, err
	}
	if err = data.RefreshOrderExpirationWithTransaction(tx, parent.TradeId); err != nil {
		tx.Rollback()
		_ = data.UnLockTransactionByTradeId(subTradeID)
		return nil, err
	}
	if err = tx.Commit().Error; err != nil {
		tx.Rollback()
		_ = data.UnLockTransactionByTradeId(subTradeID)
		return nil, err
	}

	_ = data.MarkOrderSelected(parent.TradeId)

	return buildCheckoutResponse(subOrder), nil
}

func completeWaitSelectOrder(parent *mdb.Orders, token string, network string) (*response.CheckoutCounterResponse, error) {
	if !data.IsChainEnabled(network) {
		return nil, constant.ChainNotEnabled
	}
	if _, err := ensureEnabledOrderAsset(network, token); err != nil {
		return nil, err
	}

	rate := config.GetRateForCoin(strings.ToLower(token), strings.ToLower(parent.Currency))
	if rate <= 0 {
		return nil, constant.RateAmountErr
	}
	decimalPayAmount := decimal.NewFromFloat(parent.Amount)
	decimalTokenAmount := decimalPayAmount.Mul(decimal.NewFromFloat(rate))
	if decimalTokenAmount.Cmp(decimal.NewFromFloat(UsdtMinimumPaymentAmount)) == -1 {
		return nil, constant.PayAmountErr
	}

	walletAddress, err := data.GetAvailableWalletAddressByNetwork(network)
	if err != nil {
		return nil, err
	}
	if len(walletAddress) <= 0 {
		return nil, constant.NotAvailableWalletAddress
	}

	amount := math.MustParsePrecFloat64(decimalTokenAmount.InexactFloat64(), data.GetAmountPrecision())
	availableAddress, availableAmount, err := ReserveAvailableWalletAndAmount(parent.TradeId, network, token, amount, walletAddress)
	if err != nil {
		return nil, err
	}
	if availableAddress == "" {
		return nil, constant.NotAvailableAmountErr
	}

	updated, err := data.CompleteWaitSelectOrder(parent.TradeId, network, token, availableAddress, availableAmount)
	if err != nil {
		_ = data.UnLockTransactionByTradeId(parent.TradeId)
		return nil, err
	}
	if !updated {
		_ = data.UnLockTransactionByTradeId(parent.TradeId)
		return nil, constant.OrderStatusConflict
	}

	order, err := data.GetOrderInfoByTradeId(parent.TradeId)
	if err != nil {
		return nil, err
	}
	return buildCheckoutResponse(order), nil
}

func buildCheckoutResponse(order *mdb.Orders) *response.CheckoutCounterResponse {
	paymentType := mdb.PaymentTypeGmpay
	if isEPayOrder(order) {
		paymentType = mdb.PaymentTypeEpay
	}
	return &response.CheckoutCounterResponse{
		CollectionMode: order.CollectionMode, MinimumPayment: order.MinimumPayment,
		PaidCryptoAmount: order.PaidCryptoAmount, PaidAmount: order.PaidAmount,
		TradeId:        order.TradeId,
		Amount:         order.Amount,
		ActualAmount:   order.ActualAmount,
		Token:          order.Token,
		Currency:       order.Currency,
		ReceiveAddress: order.ReceiveAddress,
		Network:        order.Network,
		Status:         order.Status,
		PaymentType:    strings.ToLower(paymentType),
		ExpirationTime: orderExpiryMillis(order),
		RedirectUrl:    buildPublicRedirectURL(order),
		CreatedAt:      order.CreatedAt.TimestampMilli(),
		ServerTime:     carbon.Now().TimestampMilli(),
		IsSelected:     order.IsSelected,
	}
}

func completeWaitSelectOkPayOrder(parent *mdb.Orders, token string) (*response.CheckoutCounterResponse, error) {
	rate := config.GetRateForCoin(strings.ToLower(token), strings.ToLower(parent.Currency))
	if rate <= 0 {
		return nil, constant.RateAmountErr
	}
	decimalPayAmount := decimal.NewFromFloat(parent.Amount)
	decimalTokenAmount := decimalPayAmount.Mul(decimal.NewFromFloat(rate))
	if decimalTokenAmount.Cmp(decimal.NewFromFloat(UsdtMinimumPaymentAmount)) == -1 {
		return nil, constant.PayAmountErr
	}

	amount := math.MustParsePrecFloat64(decimalTokenAmount.InexactFloat64(), data.GetAmountPrecision())
	// Publish the provider channel before the HTTP request so an early verified
	// callback sees the same durable amount and identity as a retry after restart.
	data.CollectionMu.Lock()
	err := dao.Mdb.Transaction(func(tx *gorm.DB) error {
		var fresh mdb.Orders
		if err := tx.Where("trade_id = ?", parent.TradeId).First(&fresh).Error; err != nil {
			return err
		}
		if fresh.Status != mdb.StatusWaitSelect || !data.PaymentDeadline(&fresh).After(time.Now()) {
			return constant.OrderStatusConflict
		}
		var prior mdb.ProviderOrder
		if err := tx.Where("trade_id = ? AND provider = ?", parent.TradeId, mdb.PaymentProviderOkPay).Find(&prior).Error; err != nil {
			return err
		}
		if prior.ID != 0 {
			if !strings.EqualFold(prior.Coin, token) {
				return constant.OrderStatusConflict
			}
			amount = prior.Amount // An interrupted older attempt already fixed the quote.
		}
		updated, err := data.CompleteWaitSelectOkPayOrderWithTransaction(tx, parent.TradeId, token, amount)
		if err != nil {
			return err
		}
		if !updated {
			return constant.OrderStatusConflict
		}
		if prior.ID != 0 {
			return nil
		}
		return data.CreateProviderOrderWithTransaction(tx, &mdb.ProviderOrder{TradeId: parent.TradeId, Provider: mdb.PaymentProviderOkPay,
			Amount: amount, Coin: token, Status: mdb.ProviderOrderStatusCreating})
	})
	data.CollectionMu.Unlock()
	if err != nil {
		return nil, err
	}
	return resumeOkPayCheckout(parent.TradeId)
}

func switchToOkPay(parent *mdb.Orders, token string) (*response.CheckoutCounterResponse, error) {
	if parent.CollectionMode == mdb.CollectionExclusive {
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
	}

	if !data.GetOkPayEnabled() {
		return nil, constant.PaymentProviderNotEnabled
	}
	if data.GetOkPayShopID() == "" || data.GetOkPayShopToken() == "" || data.GetOkPayAPIURL() == "" || data.GetOkPayCallbackURL() == "" {
		return nil, constant.PaymentProviderConfigErr
	}
	if !okPayTokenAllowed(token) {
		return nil, constant.PaymentProviderNotSupport
	}

	if parent.Status == mdb.StatusWaitSelect {
		return completeWaitSelectOkPayOrder(parent, token)
	}

	if parent.PayProvider == mdb.PaymentProviderOkPay && strings.EqualFold(parent.Token, token) {
		return resumeOkPayCheckout(parent.TradeId)
	}

	// Include old failed attempts: they own a stable provider unique_id and
	// must be recovered, rather than consuming another child slot.
	var existing mdb.Orders
	if err := dao.Mdb.Where("parent_trade_id = ? AND token = ? AND pay_provider = ?", parent.TradeId, token, mdb.PaymentProviderOkPay).First(&existing).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if existing.ID > 0 {
		return resumeOkPayCheckout(existing.TradeId)
	}

	count, err := data.CountSubOrders(parent.TradeId)
	if err != nil {
		return nil, err
	}
	if count >= MaxSubOrders {
		return nil, constant.SubOrderLimitExceeded
	}

	rate := config.GetRateForCoin(strings.ToLower(token), strings.ToLower(parent.Currency))
	if rate <= 0 {
		return nil, constant.RateAmountErr
	}
	decimalPayAmount := decimal.NewFromFloat(parent.Amount)
	decimalTokenAmount := decimalPayAmount.Mul(decimal.NewFromFloat(rate))
	if decimalTokenAmount.Cmp(decimal.NewFromFloat(UsdtMinimumPaymentAmount)) == -1 {
		return nil, constant.PayAmountErr
	}

	subTradeID := GenerateCode()
	amount := math.MustParsePrecFloat64(decimalTokenAmount.InexactFloat64(), data.GetAmountPrecision())

	subOrder := &mdb.Orders{
		TradeId:          subTradeID,
		OrderId:          subTradeID,
		ParentTradeId:    parent.TradeId,
		Amount:           parent.Amount,
		Currency:         parent.Currency,
		ActualAmount:     amount,
		ReceiveAddress:   "OKPAY",
		Token:            token,
		Network:          mdb.NetworkTron,
		Status:           mdb.StatusWaitPay,
		IsSelected:       true,
		NotifyUrl:        "",
		RedirectUrl:      parent.RedirectUrl,
		Name:             parent.Name,
		EpayType:         parent.EpayType,
		CallBackConfirm:  mdb.CallBackConfirmOk,
		PaymentType:      parent.PaymentType,
		PayProvider:      mdb.PaymentProviderOkPay,
		ApiKeyID:         parent.ApiKeyID,
		CollectionMode:   parent.CollectionMode,
		PaymentExpiresAt: parent.PaymentExpiresAt,
	}
	if subOrder.CollectionMode != mdb.CollectionExclusive {
		subOrder.PaymentExpiresAt = nil // legacy channel switches restart the child deadline
	}
	data.CollectionMu.Lock()
	err = dao.Mdb.Transaction(func(tx *gorm.DB) error {
		updated, err := data.MarkProviderSwitchParentSelectedWithTransaction(tx, parent.TradeId)
		if err != nil {
			return err
		}
		if !updated {
			return constant.OrderStatusConflict
		}
		if err := data.CreateOrderWithTransaction(tx, subOrder); err != nil {
			return err
		}
		return data.CreateProviderOrderWithTransaction(tx, &mdb.ProviderOrder{TradeId: subTradeID, Provider: mdb.PaymentProviderOkPay,
			Amount: amount, Coin: token, Status: mdb.ProviderOrderStatusCreating})
	})
	data.CollectionMu.Unlock()
	if err != nil {
		return nil, err
	}
	return resumeOkPayCheckout(subTradeID)
}

func okPayTokenAllowed(token string) bool {
	for _, item := range data.GetOkPayAllowTokens() {
		if strings.EqualFold(item, token) {
			return true
		}
	}
	return false
}
