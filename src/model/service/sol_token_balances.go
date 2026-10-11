package service

import (
	"fmt"
	"strconv"

	"github.com/gagliardetto/solana-go"
	"github.com/tidwall/gjson"
)

// Unchecked SPL transfers omit mint/decimals. Closed token accounts may only
// have pre-balances, while newly created accounts may only have post-balances.
// Accept either endpoint, but never default missing metadata to zero or hide
// malformed/conflicting rows by falling back to another endpoint or snapshot.
func resolveSplTransferAsset(source, destination string, body []byte) (string, int, error) {
	keys := gjson.GetBytes(body, "result.transaction.message.accountKeys")
	if source == "" || destination == "" || !keys.IsArray() {
		return "", 0, fmt.Errorf("missing transfer endpoints or account keys")
	}
	accounts := keys.Array()
	sourceIndex, destinationIndex := -1, -1
	for i, account := range accounts {
		pubkey := account.Get("pubkey")
		if pubkey.Type != gjson.String || pubkey.String() == "" {
			return "", 0, fmt.Errorf("invalid parsed account key at index %d", i)
		}
		if pubkey.String() == source {
			sourceIndex = i
		}
		if pubkey.String() == destination {
			destinationIndex = i
		}
	}
	if sourceIndex < 0 || destinationIndex < 0 {
		return "", 0, fmt.Errorf("transfer endpoint missing from account keys")
	}
	mint, decimals, found := "", 0, false
	for _, field := range []string{"postTokenBalances", "preTokenBalances"} {
		balances := gjson.GetBytes(body, "result.meta."+field)
		if !balances.Exists() || balances.Type == gjson.Null {
			continue
		}
		if !balances.IsArray() {
			return "", 0, fmt.Errorf("invalid %s array", field)
		}
		for _, balance := range balances.Array() {
			indexField := balance.Get("accountIndex")
			if indexField.Type != gjson.Number {
				return "", 0, fmt.Errorf("missing or invalid %s accountIndex", field)
			}
			index, err := strconv.ParseUint(indexField.Raw, 10, 64)
			if err != nil || index >= uint64(len(accounts)) {
				return "", 0, fmt.Errorf("out-of-range or invalid %s accountIndex", field)
			}
			if int(index) != sourceIndex && int(index) != destinationIndex {
				continue
			}
			mintField := balance.Get("mint")
			precisionField := balance.Get("uiTokenAmount.decimals")
			if mintField.Type != gjson.String || mintField.String() == "" || precisionField.Type != gjson.Number {
				return "", 0, fmt.Errorf("missing or invalid %s mint/decimals for account %d", field, index)
			}
			if _, err := solana.PublicKeyFromBase58(mintField.String()); err != nil {
				return "", 0, fmt.Errorf("invalid %s mint for account %d: %w", field, index, err)
			}
			precision, err := strconv.ParseUint(precisionField.Raw, 10, 8)
			if err != nil {
				return "", 0, fmt.Errorf("invalid %s decimals for account %d", field, index)
			}
			if found && (mint != mintField.String() || decimals != int(precision)) {
				return "", 0, fmt.Errorf("conflicting transfer mint/decimals across token balances")
			}
			mint, decimals, found = mintField.String(), int(precision), true
		}
	}
	if !found {
		return "", 0, fmt.Errorf("could not determine mint for transfer: source=%s dest=%s", source, destination)
	}
	return mint, decimals, nil
}
