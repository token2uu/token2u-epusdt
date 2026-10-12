package service

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/GMWalletApp/epusdt/model/mdb"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/tidwall/gjson"
)

// ParseAptosIndexedDeposits verifies indexed events against fullnode evidence.
// A successful Deposit credits the recipient even without a Withdraw (minting).
// Callers must validate the transaction envelope and resource shapes first.
func ParseAptosIndexedDeposits(body []byte, owner string, rows []AptosIndexedDeposit, tokens []mdb.ChainToken) ([]MoveObservedTransfer, error) {
	owner, err := addressutil.NormalizeMoveAddress(owner)
	if err != nil {
		return nil, err
	}
	tx := gjson.ParseBytes(body)
	events := buildAptosFungibleEvents(tx, buildAptosTransferContext(tx), tokens)
	byIndex := make(map[int]aptosFungibleEvent, len(events))
	for _, event := range events {
		byIndex[event.eventIndex] = event
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no indexed deposits to verify")
	}
	for _, row := range rows {
		pos, positionErr := row.Position()
		amount, amountOK := new(big.Int).SetString(row.Amount.String(), 10)
		rowOwner, ownerErr := addressutil.NormalizeMoveAddress(row.Owner)
		event, found := byIndex[int(pos.Index)]
		if positionErr != nil || pos.Version != tx.Get("version").Int() || ownerErr != nil || rowOwner != owner ||
			!found || row.Type != "0x1::fungible_asset::Deposit" || event.action != "deposit" || event.owner != owner ||
			event.token == nil || !addressutil.MoveAssetMatches(row.Asset, event.token.ContractAddress) ||
			!amountOK || amount.Sign() <= 0 || event.rawAmount.Cmp(amount) != 0 {
			return nil, fmt.Errorf("RPC event does not match indexed deposit version=%d index=%d", pos.Version, pos.Index)
		}
	}
	// Parse every deposit in the verified transaction, including events that land
	// on the next indexer page. Stable event receipt keys make overlap idempotent.
	var out []MoveObservedTransfer
	wallets := map[string]struct{}{owner: {}}
	for _, event := range events {
		if event.action == "deposit" {
			appendAptosObservedTransfer(&out, strings.TrimSpace(tx.Get("hash").String()), tx.Get("version").Int(), parseAptosTimestampMs(tx.Get("timestamp")), event, wallets)
		}
	}
	return out, nil
}
