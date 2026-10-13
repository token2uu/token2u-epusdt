package data

import (
	"math/big"

	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// Called under CollectionMu in the settlement transaction, after RPC verification.
// Exact and mismatched payments require the same historical ownership proof.
func manualPaymentAmount(tx *gorm.DB, order *mdb.Orders, evidence *request.ManualPaymentEvidence) (decimal.Decimal, error) {
	reject := constant.ManualPaymentVerifyErr
	raw, ok := new(big.Int).SetString(evidence.RawAmount, 10)
	if !ok || raw.Sign() <= 0 || evidence.Decimals < 0 || evidence.Decimals > 36 || evidence.BlockTimeMs <= 0 ||
		(order.PayProvider != "" && order.PayProvider != mdb.PaymentProviderOnChain) || order.CollectionMode == mdb.CollectionExclusive {
		return decimal.Zero, reject
	}
	p := ObservedPayment{Network: normalizeLockNetwork(order.Network), Token: normalizeLockToken(order.Token),
		Address: normalizeLockAddress(order.Network, order.ReceiveAddress), RawAmount: raw, Decimals: evidence.Decimals,
		BlockTimeMs: evidence.BlockTimeMs, AssetID: evidence.AssetID}
	if normalizeLockNetwork(evidence.Network) != p.Network || normalizeLockToken(evidence.Token) != p.Token || normalizeLockAddress(p.Network, evidence.Address) != p.Address {
		return decimal.Zero, reject
	}
	amount := decimal.NewFromBigInt(raw, -int32(p.Decimals))
	var asset mdb.ChainToken
	if err := tx.Where("network = ? AND UPPER(symbol) = ? AND enabled = ?", p.Network, p.Token, true).First(&asset).Error; err != nil {
		return decimal.Zero, err
	}
	if PaymentAssetID(p.Network, asset.ContractAddress) != PaymentAssetID(p.Network, p.AssetID) || asset.Decimals != p.Decimals || amount.LessThan(decimal.NewFromFloat(asset.MinAmount)) {
		return decimal.Zero, reject
	}
	candidates, ends, err := legacyPaymentCandidates(tx, p)
	if err != nil {
		return decimal.Zero, err
	}
	if len(candidates) != 1 || candidates[0].ID != order.ID {
		return decimal.Zero, reject
	}
	start, end := paymentChainTimeInterval(p)
	if start < PaymentAssignmentTimeMs(order) || end >= ends[order.ID] {
		return decimal.Zero, reject
	}
	return amount, nil
}
