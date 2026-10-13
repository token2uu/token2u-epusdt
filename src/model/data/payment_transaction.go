package data

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/GMWalletApp/epusdt/model/mdb"
	"gorm.io/gorm"
)

// Only fixed-width hex transaction hashes have case/prefix aliases. Solana
// signatures, TON composite IDs and hosted-provider IDs retain their identity.
func paymentHexTransactionBody(network, txID string) (string, bool) {
	switch normalizeLockNetwork(network) {
	case mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma, mdb.NetworkAptos, mdb.NetworkTron:
	default:
		return "", false
	}
	body := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(txID)), "0x")
	if len(body) != 64 {
		return "", false
	}
	if _, err := hex.DecodeString(body); err != nil {
		return "", false
	}
	return body, true
}

func CanonicalPaymentTransactionID(network, txID string) string {
	if body, ok := paymentHexTransactionBody(network, txID); ok {
		if normalizeLockNetwork(network) == mdb.NetworkTron {
			return body
		}
		return "0x" + body
	}
	return strings.TrimSpace(txID)
}

// Use within the same transaction/CollectionMu as settlement. RPC-time checks
// alone cannot prevent a concurrent scanner from claiming the payment first.
func GetOrderByPaymentTransactionWithTransaction(tx *gorm.DB, network, txID string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	q := paymentTransactionQuery(tx.Model(order), network, txID, "block_transaction_id")
	err := q.Limit(1).Find(order).Error
	return order, err
}

func paymentTransactionQuery(tx *gorm.DB, network, txID, column string) *gorm.DB {
	q := tx.Where("network = ?", normalizeLockNetwork(network))
	if body, ok := paymentHexTransactionBody(network, txID); ok {
		q = q.Where("LOWER("+column+") IN ?", []string{body, "0x" + body})
	} else {
		q = q.Where(column+" = ?", strings.TrimSpace(txID))
	}
	return q
}

func paymentEventKey(network, txID, eventID string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(normalizeLockNetwork(network)+"\x00"+CanonicalPaymentTransactionID(network, txID)+"\x00"+eventID)))
}

// Event receipts permit distinct transfers in one transaction. Pre-upgrade and
// manual payments without an event receipt conservatively claim the whole tx.
// Call again inside the settlement transaction under CollectionMu after RPC.
func PaymentEventAlreadyClaimed(tx *gorm.DB, network, txID, eventID string) (bool, error) {
	return paymentClaimed(tx, network, txID, eventID, false)
}

// Hash-only prechecks can reject legacy settlements with no event evidence,
// but must let the verifier resolve independent transfers in known receipts.
func PaymentTransactionHasUntrackedSettlement(tx *gorm.DB, network, txID string) (bool, error) {
	return paymentClaimed(tx, network, txID, "", true)
}

func paymentClaimed(tx *gorm.DB, network, txID, eventID string, onlyUntracked bool) (bool, error) {
	var receipts []mdb.PaymentReceipt
	if err := paymentTransactionQuery(tx.Model(&mdb.PaymentReceipt{}), network, txID, "tx_hash").Find(&receipts).Error; err != nil {
		return false, err
	}
	known := make(map[string]bool)
	for _, receipt := range receipts {
		if !onlyUntracked && eventID != "" && receipt.EventID == eventID {
			return true, nil
		}
		if receipt.Status == "settled" {
			if !onlyUntracked && eventID == "" {
				return true, nil
			}
			known[receipt.TradeID] = receipt.EventID != ""
		}
	}
	var orders []mdb.Orders
	if err := paymentTransactionQuery(tx.Model(&mdb.Orders{}), network, txID, "block_transaction_id").Where("status = ?", mdb.StatusPaySuccess).Find(&orders).Error; err != nil {
		return false, err
	}
	for _, order := range orders {
		if (!onlyUntracked && eventID == "") || !known[order.TradeId] {
			return true, nil
		}
	}
	return false, nil
}
