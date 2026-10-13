package service

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/GMWalletApp/epusdt/util/log"
)

var gProcessedMoveTx sync.Map  // transfer-level key -> unix timestamp
var gProcessingMoveTx sync.Map // in-flight transfers are never acknowledged as processed

var processMoveOrder = OrderProcessing

type MoveObservedTransfer struct {
	AssetID        string
	Network        string
	ReceiveAddress string
	Token          string
	RawAmount      *big.Int
	Decimals       int
	MinAmount      float64
	Amount         float64
	TxID           string
	BlockTimeMs    int64
	Version        int64
	TransferKey    string
}

func ProcessMoveObservedTransferResult(transfer MoveObservedTransfer) error {
	cacheKey := MoveTransferCacheKey(transfer)
	if _, loaded := gProcessingMoveTx.LoadOrStore(cacheKey, struct{}{}); loaded {
		return fmt.Errorf("Move transfer %s is still being processed", cacheKey)
	}
	defer gProcessingMoveTx.Delete(cacheKey)
	observed := data.ObservedPayment{Network: transfer.Network, Address: transfer.ReceiveAddress,
		Token: transfer.Token, AssetID: transfer.AssetID, RawAmount: transfer.RawAmount, Decimals: transfer.Decimals,
		TxHash: transfer.TxID, EventID: cacheKey, BlockTimeMs: transfer.BlockTimeMs}
	handled, collectionErr := processObservedPayment(observed)
	if collectionErr != nil || handled {
		return collectionErr
	}
	if _, loaded := gProcessedMoveTx.Load(cacheKey); loaded {
		return nil
	}
	err := processLegacyMoveObservedTransfer(transfer, observed)
	if err == nil {
		gProcessedMoveTx.Store(cacheKey, time.Now().Unix())
	}
	return err
}

func processLegacyMoveObservedTransfer(transfer MoveObservedTransfer, observed data.ObservedPayment) error {
	net := moveLogLabel(transfer.Network)
	if transfer.RawAmount == nil || transfer.RawAmount.Sign() <= 0 || transfer.Amount <= 0 {
		return nil
	}
	if transfer.MinAmount > 0 && transfer.Amount < transfer.MinAmount {
		log.Sugar.Debugf("[%s-%s][%s] skip below min amount tx=%s amount=%.2f min=%.2f", net, transfer.Token, transfer.ReceiveAddress, transfer.TxID, transfer.Amount, transfer.MinAmount)
		return nil
	}

	matchResult, err := data.GetTradeIdByWalletAddressAndAmountAndTokenWithEpayFallback(transfer.Network, transfer.ReceiveAddress, transfer.Token, transfer.Amount)
	if err != nil {
		log.Sugar.Errorf("[%s-%s][%s] lock lookup failed tx=%s: %v", net, transfer.Token, transfer.ReceiveAddress, transfer.TxID, err)
		return err
	}
	tradeID := matchResult.TradeId
	if tradeID == "" {
		if _, err := processObservedPayment(observed); err != nil {
			return err
		}
		raw := ""
		if transfer.RawAmount != nil {
			raw = transfer.RawAmount.String()
		}
		log.Sugar.Infof("[%s-%s][%s] skip unmatched tx=%s amount=%.8f raw=%s decimals=%d", net, transfer.Token, transfer.ReceiveAddress, transfer.TxID, transfer.Amount, raw, transfer.Decimals)
		return nil
	}
	if matchResult.IsEpayFallback {
		log.Sugar.Infof("[%s-%s][%s] epay fallback match trade_id=%s expected_amount!=%.8f", net, transfer.Token, transfer.ReceiveAddress, tradeID, transfer.Amount)
	}
	order, err := data.GetOrderInfoByTradeId(tradeID)
	if err != nil {
		log.Sugar.Errorf("[%s-%s][%s] load order trade_id=%s tx=%s: %v", net, transfer.Token, transfer.ReceiveAddress, tradeID, transfer.TxID, err)
		return err
	}
	if transfer.BlockTimeMs >= data.PaymentDeadline(order).UnixMilli() {
		log.Sugar.Warnf("[%s] skip payment outside order deadline trade_id=%s tx=%s", net, tradeID, transfer.TxID)
		return nil
	}
	if err := EnsureMoveTransferMatchesOrder(order, transfer); err != nil {
		var mismatch *AmountMismatchError
		if !matchResult.IsEpayFallback || !errors.As(err, &mismatch) {
			log.Sugar.Warnf("[%s-%s][%s] skip tx=%s trade_id=%s: %v", net, transfer.Token, transfer.ReceiveAddress, transfer.TxID, tradeID, err)
			return nil
		}
	}
	req := &request.OrderProcessingRequest{
		ReceiveAddress:     transfer.ReceiveAddress,
		Token:              transfer.Token,
		Network:            transfer.Network,
		TradeId:            tradeID,
		Amount:             transfer.Amount,
		BlockTransactionId: transfer.TxID,
	}
	if matchResult.IsEpayFallback {
		req.PaidAmount = transfer.Amount
	}
	if err = processMoveOrder(req); err != nil {
		if errors.Is(err, constant.OrderStatusConflict) {
			handled, recoveryErr := processObservedPayment(observed)
			if recoveryErr != nil || !handled {
				if recoveryErr != nil {
					return recoveryErr
				}
				return err
			}
			return nil
		}
		if errors.Is(err, constant.OrderBlockAlreadyProcess) {
			log.Sugar.Infof("[%s-%s][%s] already resolved trade_id=%s tx=%s reason=%v", net, transfer.Token, transfer.ReceiveAddress, tradeID, transfer.TxID, err)
			return nil
		}
		log.Sugar.Errorf("[%s-%s][%s] OrderProcessing failed trade_id=%s tx=%s: %v", net, transfer.Token, transfer.ReceiveAddress, tradeID, transfer.TxID, err)
		return err
	}
	sendPaymentNotification(order)
	log.Sugar.Infof("[%s-%s][%s] payment processed trade_id=%s tx=%s", net, transfer.Token, transfer.ReceiveAddress, tradeID, transfer.TxID)
	return nil
}

func MoveTransferCacheKey(transfer MoveObservedTransfer) string {
	if transfer.TransferKey != "" {
		return strings.ToLower(strings.TrimSpace(transfer.TransferKey))
	}
	receive := strings.ToLower(strings.TrimSpace(transfer.ReceiveAddress))
	token := strings.ToUpper(strings.TrimSpace(transfer.Token))
	raw := ""
	if transfer.RawAmount != nil {
		raw = transfer.RawAmount.String()
	}
	switch strings.ToLower(strings.TrimSpace(transfer.Network)) {
	case mdb.NetworkAptos:
		return fmt.Sprintf("aptos:%d:%s:%s:%s", transfer.Version, receive, token, raw)
	default:
		return strings.ToLower(strings.TrimSpace(transfer.Network)) + ":" + strings.TrimSpace(transfer.TxID)
	}
}

func EnsureMoveTransferMatchesOrder(order *mdb.Orders, transfer MoveObservedTransfer) error {
	if order == nil || order.ID == 0 {
		return fmt.Errorf("order not found")
	}
	if strings.ToLower(strings.TrimSpace(order.Network)) != strings.ToLower(strings.TrimSpace(transfer.Network)) {
		return fmt.Errorf("network mismatch")
	}
	orderAddr, err := addressutil.NormalizeMoveAddress(order.ReceiveAddress)
	if err != nil {
		return fmt.Errorf("invalid order receive address: %w", err)
	}
	transferAddr, err := addressutil.NormalizeMoveAddress(transfer.ReceiveAddress)
	if err != nil {
		return fmt.Errorf("invalid transfer receive address: %w", err)
	}
	if orderAddr != transferAddr {
		return fmt.Errorf("transaction recipient mismatch")
	}
	if !strings.EqualFold(order.Token, transfer.Token) {
		return fmt.Errorf("token mismatch")
	}
	if transfer.BlockTimeMs <= 0 {
		return fmt.Errorf("transaction block time missing")
	}
	if transfer.BlockTimeMs < data.PaymentAssignmentTimeMs(order) {
		return fmt.Errorf("transaction predates the order")
	}
	if !manualOrderAmountMatches(order, transfer.RawAmount, transfer.Decimals) {
		return newAmountMismatch(transfer.RawAmount, transfer.Decimals, transfer.BlockTimeMs, transfer.AssetID, MoveTransferCacheKey(transfer))
	}
	return nil
}

func moveWalletWatched(address string, wallets map[string]struct{}) bool {
	if address == "" || len(wallets) == 0 {
		return false
	}
	_, ok := wallets[address]
	return ok
}

func moveAptosTransferKey(version int64, eventIndex int, receive, token string, raw *big.Int) string {
	rawText := ""
	if raw != nil {
		rawText = raw.String()
	}
	return fmt.Sprintf("aptos:%d:%d:%s:%s:%s", version, eventIndex, strings.ToLower(strings.TrimSpace(receive)), strings.ToUpper(strings.TrimSpace(token)), rawText)
}

func resolveMoveToken(network, assetID string, tokens []mdb.ChainToken) *mdb.ChainToken {
	assetID = addressutil.NormalizeMoveAssetID(assetID)
	for i := range tokens {
		contract := strings.TrimSpace(tokens[i].ContractAddress)
		if contract == "" {
			continue
		}
		if addressutil.MoveAssetMatches(assetID, contract) {
			return &tokens[i]
		}
	}
	return nil
}

func firstMoveAddress(values ...string) string {
	for _, v := range values {
		if normalized, err := addressutil.NormalizeMoveAddress(v); err == nil {
			return normalized
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func parseSignedBigInt(raw string) (*big.Int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	out, ok := new(big.Int).SetString(raw, 10)
	return out, ok
}

func rawToFloat(raw *big.Int, decimals int) float64 {
	if raw == nil {
		return 0
	}
	value, _ := new(big.Rat).SetFrac(new(big.Int).Set(raw), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)).Float64()
	return value
}

func recordMoveRPCResult(network string, errp *error) {
	if errp != nil && *errp != nil {
		data.RecordRpcFailure(network)
		return
	}
	data.RecordRpcSuccess(network)
}

func moveLogLabel(network string) string {
	switch strings.ToLower(strings.TrimSpace(network)) {
	case mdb.NetworkAptos:
		return "APTOS"
	default:
		return strings.ToUpper(network)
	}
}
