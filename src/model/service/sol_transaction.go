package service

import (
	"fmt"
	"strconv"

	"github.com/gagliardetto/solana-go"
	"github.com/tidwall/gjson"
)

// Parse the whole response before applying any payment. A missing array must
// not look like an empty array and a parse failure must not consume a signature.
// Keep nil entries for non-transfers: receipt event indexes must stay stable.
func parseSolTransactionTransfers(body []byte, watchedAddresses []string) ([]*TransferInfo, error) {
	if !gjson.ValidBytes(body) {
		return nil, fmt.Errorf("invalid transaction JSON")
	}
	result := gjson.GetBytes(body, "result")
	if !result.IsObject() || !result.Get("meta").IsObject() || !result.Get("meta.err").Exists() {
		return nil, fmt.Errorf("missing transaction result or status metadata")
	}
	if result.Get("meta.err").Type != gjson.Null {
		return nil, nil // A definitively failed transaction cannot credit a payment.
	}
	top := result.Get("transaction.message.instructions")
	if !top.IsArray() {
		return nil, fmt.Errorf("missing or invalid transaction instructions")
	}
	instructions := top.Array()
	// RPC permits omitted/null innerInstructions when recording is unavailable.
	// When provided, every group must contain an actual instruction array.
	inner := result.Get("meta.innerInstructions")
	if inner.Exists() && inner.Type != gjson.Null {
		if !inner.IsArray() {
			return nil, fmt.Errorf("invalid inner instructions")
		}
		for _, group := range inner.Array() {
			if !group.IsObject() || !group.Get("instructions").IsArray() {
				return nil, fmt.Errorf("missing or invalid inner instruction group")
			}
			instructions = append(instructions, group.Get("instructions").Array()...)
		}
	}
	transfers := make([]*TransferInfo, len(instructions))
	for i, instruction := range instructions {
		if err := validateSolPaymentInstruction(instruction); err != nil {
			return nil, fmt.Errorf("instruction %d: %w", i, err)
		}
		transfer, err := ParseTransferInfoFromInstruction(instruction, body)
		if err != nil {
			// A temporary token account created and closed in this transaction
			// has neither a pre- nor a post-balance. Failure to infer its mint
			// must not block an independent payment. Only exclude an unchecked
			// SPL transfer with a validated destination outside this scan's
			// owner and configured ATAs; incoming or unidentified transfers
			// still fail the entire parse. Leave its event index occupied.
			if unrelatedSplTransfer(instruction, watchedAddresses) {
				continue
			}
			return nil, fmt.Errorf("instruction %d: %w", i, err)
		}
		if transfer != nil && (transfer.Source == "" || transfer.Destination == "" || transfer.Mint == "") {
			return nil, fmt.Errorf("instruction %d: missing transfer address or mint", i)
		}
		transfers[i] = transfer
	}
	return transfers, nil
}

func unrelatedSplTransfer(instruction gjson.Result, watchedAddresses []string) bool {
	program := instruction.Get("programId").String()
	if len(watchedAddresses) == 0 || (program != TokenProgramID && program != Token2022ProgramID) || instruction.Get("parsed.type").String() != "transfer" {
		return false
	}
	destination := instruction.Get("parsed.info.destination")
	if destination.Type != gjson.String || destination.String() == "" {
		return false
	}
	if _, err := solana.PublicKeyFromBase58(destination.String()); err != nil {
		return false // A malformed address is not proof of an unrelated transfer.
	}
	for _, address := range watchedAddresses {
		if destination.String() == address {
			return false
		}
	}
	return true
}

func validateSolPaymentInstruction(instruction gjson.Result) error {
	program := instruction.Get("programId")
	if !instruction.IsObject() || program.Type != gjson.String || program.String() == "" {
		return fmt.Errorf("missing instruction programId")
	}
	parsed := instruction.Get("parsed")
	if !parsed.Exists() || parsed.Type == gjson.Null {
		// Unknown programs are legitimately returned as partially decoded data.
		if instruction.Get("data").Type != gjson.String || !instruction.Get("accounts").IsArray() {
			return fmt.Errorf("missing parsed instruction or raw data/accounts")
		}
		return nil
	}
	knownProgram := program.String() == SystemProgramID || program.String() == TokenProgramID || program.String() == Token2022ProgramID
	if !knownProgram {
		return nil // E.g. the memo program can return a parsed string.
	}
	if !parsed.IsObject() || parsed.Get("type").Type != gjson.String || parsed.Get("type").String() == "" {
		return fmt.Errorf("missing parsed instruction type")
	}
	kind := parsed.Get("type").String()
	amountPath, amountType := "", gjson.String
	switch {
	case program.String() == SystemProgramID && kind == "transfer":
		amountPath, amountType = "lamports", gjson.Number
	case program.String() != SystemProgramID && kind == "transfer":
		amountPath = "amount"
	case program.String() != SystemProgramID && kind == "transferChecked":
		amountPath = "tokenAmount.amount"
		decimals := parsed.Get("info.tokenAmount.decimals")
		if decimals.Type != gjson.Number {
			return fmt.Errorf("missing token decimals")
		}
		if _, err := strconv.ParseUint(decimals.Raw, 10, 8); err != nil {
			return fmt.Errorf("invalid token decimals")
		}
	default:
		return nil
	}
	for _, field := range []string{"source", "destination"} {
		address := parsed.Get("info." + field)
		if address.Type != gjson.String || address.String() == "" {
			return fmt.Errorf("missing or invalid transfer %s", field)
		}
	}
	amount := parsed.Get("info." + amountPath)
	if amount.Type != amountType {
		return fmt.Errorf("missing transfer amount")
	}
	if _, err := strconv.ParseUint(amount.String(), 10, 64); err != nil {
		return fmt.Errorf("invalid transfer amount: %w", err)
	}
	return nil
}
