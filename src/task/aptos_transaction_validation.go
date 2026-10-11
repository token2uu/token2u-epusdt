package task

import (
	"fmt"
	"strconv"
	"strings"

	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/tidwall/gjson"
)

// Run before parsing or applying any payment in the range. Contiguous versions
// prove coverage, not that a successful response contains complete transactions.
func validateAptosLedgerContents(body []byte) error {
	for _, tx := range gjson.ParseBytes(body).Array() {
		if err := validateAptosPaymentContent(tx); err != nil {
			return fmt.Errorf("incomplete transaction version=%s: %w", tx.Get("version").String(), err)
		}
	}
	return nil
}

func validateAptosPaymentContent(tx gjson.Result) error {
	typeField := tx.Get("type")
	if typeField.Type != gjson.String || strings.TrimSpace(typeField.String()) == "" {
		return fmt.Errorf("missing or invalid transaction type")
	}
	if typeField.String() != "user_transaction" {
		return nil // The payment parser only consumes user transactions.
	}
	status := tx.Get("success")
	if status.Type != gjson.True && status.Type != gjson.False {
		return fmt.Errorf("missing or invalid success flag")
	}
	if status.Type == gjson.False {
		return nil // An explicitly failed transaction cannot contain a payment.
	}
	hash, timestamp := tx.Get("hash"), tx.Get("timestamp")
	if hash.Type != gjson.String || strings.TrimSpace(hash.String()) == "" {
		return fmt.Errorf("missing or invalid transaction hash")
	}
	if timestamp.Type != gjson.String {
		return fmt.Errorf("missing or invalid timestamp")
	}
	micros, err := strconv.ParseInt(timestamp.String(), 10, 64)
	if err != nil || micros < 1000 {
		return fmt.Errorf("invalid transaction timestamp")
	}
	for _, field := range []string{"events", "changes"} {
		if !tx.Get(field).IsArray() {
			return fmt.Errorf("missing or invalid %s array", field)
		}
	}
	for _, event := range tx.Get("events").Array() {
		typ := event.Get("type")
		if typ.Type != gjson.String || typ.String() == "" || !event.Get("data").Exists() {
			return fmt.Errorf("missing event type or data")
		}
		switch strings.ToLower(typ.String()) {
		case "0x1::fungible_asset::deposit", "0x1::fungible_asset::withdraw":
			amount, store := event.Get("data.amount"), event.Get("data.store")
			if amount.Type != gjson.String || store.Type != gjson.String {
				return fmt.Errorf("missing fungible event amount or store")
			}
			if _, err := strconv.ParseUint(amount.String(), 10, 64); err != nil {
				return fmt.Errorf("invalid fungible event amount: %w", err)
			}
			if _, err := addressutil.NormalizeMoveAddress(store.String()); err != nil {
				return fmt.Errorf("invalid fungible event store: %w", err)
			}
		}
	}
	for _, change := range tx.Get("changes").Array() {
		typ := change.Get("type")
		if typ.Type != gjson.String || typ.String() == "" {
			return fmt.Errorf("missing change type")
		}
		if typ.String() == "write_resource" {
			if change.Get("address").Type != gjson.String || change.Get("data.type").Type != gjson.String || !change.Get("data.data").IsObject() {
				return fmt.Errorf("incomplete resource change")
			}
			if _, err := addressutil.NormalizeMoveAddress(change.Get("address").String()); err != nil {
				return fmt.Errorf("invalid resource address: %w", err)
			}
			// These fields determine the asset and owner of each store. An
			// incomplete entry must not silently make its deposits disappear.
			var account gjson.Result
			switch change.Get("data.type").String() {
			case "0x1::fungible_asset::FungibleStore":
				account = change.Get("data.data.metadata")
				if account.IsObject() {
					account = account.Get("inner")
				}
			case "0x1::object::ObjectCore":
				account = change.Get("data.data.owner")
			default:
				continue
			}
			if account.Type != gjson.String {
				return fmt.Errorf("missing resource metadata or owner")
			}
			if _, err := addressutil.NormalizeMoveAddress(account.String()); err != nil {
				return fmt.Errorf("invalid resource metadata or owner: %w", err)
			}
		}
	}
	return nil
}
