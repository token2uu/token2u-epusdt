package task

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/GMWalletApp/epusdt/internal/troncodec"
	"github.com/tidwall/gjson"
)

// Read exactly the fields used by commitment validation, with the same parser
// and case-sensitive lookup rules. encoding/json's case-insensitive struct
// matching must not turn an unvalidated alias into a payment input.
func decodeValidatedTronBlock(raw []byte) Block {
	root := gjson.ParseBytes(raw)
	var block Block
	block.BlockID = root.Get("blockID").String()
	header := root.Get("block_header.raw_data")
	block.BlockHeader.RawData.Number = header.Get("number").Int()
	block.BlockHeader.RawData.Timestamp = header.Get("timestamp").Int()
	block.BlockHeader.RawData.WitnessAddress = header.Get("witness_address").String()
	block.BlockHeader.RawData.ParentHash = header.Get("parentHash").String()
	block.BlockHeader.RawData.Version = int(header.Get("version").Int())
	for _, value := range root.Get("transactions").Array() {
		var tx Transaction
		tx.TxID = value.Get("txID").String()
		tx.RawData.Timestamp = value.Get("raw_data.timestamp").Int()
		tx.RawData.FeeLimit = value.Get("raw_data.fee_limit").Int()
		for _, contract := range value.Get("raw_data.contract").Array() {
			tx.RawData.Contract = append(tx.RawData.Contract, struct {
				Type      string        `json:"type"`
				Parameter ContractParam `json:"parameter"`
			}{Type: contract.Get("type").String(), Parameter: ContractParam{
				TypeURL: contract.Get("parameter.type_url").String(),
				Value:   json.RawMessage(contract.Get("parameter.value").Raw),
			}})
		}
		for _, result := range value.Get("ret").Array() {
			tx.Ret = append(tx.Ret, struct {
				ContractRet string `json:"contractRet"`
			}{result.Get("contractRet").String()})
		}
		block.Transactions = append(block.Transactions, tx)
	}
	return block
}

// TRON omits transactions on legitimate empty blocks. The header's zero
// transaction root distinguishes that case from a truncated nonempty block.
// Check the entire payment input before accepting a node or writing a cursor.
func validateTronBlockResponse(raw []byte, number *int64) error {
	if !gjson.ValidBytes(raw) {
		return fmt.Errorf("invalid TRON block JSON")
	}
	block := gjson.ParseBytes(raw)
	header := block.Get("block_header.raw_data")
	height, heightErr := strconv.ParseInt(header.Get("number").Raw, 10, 64)
	stamp, timeErr := strconv.ParseInt(header.Get("timestamp").Raw, 10, 64)
	if !block.IsObject() || block.Get("blockID").Type != gjson.String || block.Get("blockID").String() == "" ||
		heightErr != nil || height < 0 || timeErr != nil || stamp <= 0 || (number != nil && height != *number) {
		return fmt.Errorf("incomplete or wrong TRON block")
	}
	root, err := hex.DecodeString(header.Get("txTrieRoot").String())
	if err != nil || len(root) != 32 {
		return fmt.Errorf("missing or invalid TRON transaction root")
	}
	emptyRoot := true
	for _, b := range root {
		emptyRoot = emptyRoot && b == 0
	}
	transactions := block.Get("transactions")
	if !transactions.Exists() {
		if emptyRoot {
			return troncodec.Verify(block)
		}
		return fmt.Errorf("TRON block is missing committed transactions")
	}
	if !transactions.IsArray() || (len(transactions.Array()) == 0) != emptyRoot {
		return fmt.Errorf("TRON transaction list does not match empty/nonempty root")
	}
	for _, tx := range transactions.Array() {
		contracts, results := tx.Get("raw_data.contract"), tx.Get("ret")
		if !tx.IsObject() || tx.Get("txID").Type != gjson.String || tx.Get("txID").String() == "" ||
			!contracts.IsArray() || len(contracts.Array()) == 0 || !results.IsArray() || len(results.Array()) == 0 {
			return fmt.Errorf("incomplete TRON transaction")
		}
		for _, result := range results.Array() {
			if result.Get("contractRet").Type != gjson.String || result.Get("contractRet").String() == "" {
				return fmt.Errorf("missing TRON transaction outcome")
			}
		}
		for _, contract := range contracts.Array() {
			kind, value := contract.Get("type"), contract.Get("parameter.value")
			if kind.Type != gjson.String || kind.String() == "" || !value.IsObject() {
				return fmt.Errorf("incomplete TRON contract")
			}
			if kind.String() != "TransferContract" && kind.String() != "TriggerSmartContract" {
				continue
			}
			if _, err := HexToTronAddress(value.Get("owner_address").String()); err != nil {
				return fmt.Errorf("invalid TRON sender")
			}
			if kind.String() == "TransferContract" {
				if _, err := HexToTronAddress(value.Get("to_address").String()); err != nil {
					return fmt.Errorf("invalid TRON recipient")
				}
				if _, err := strconv.ParseInt(value.Get("amount").Raw, 10, 64); err != nil {
					return fmt.Errorf("invalid TRON transfer amount")
				}
			} else {
				if _, err := HexToTronAddress(value.Get("contract_address").String()); err != nil {
					return fmt.Errorf("invalid TRON contract address")
				}
				// Calling a contract's receive/fallback function can legitimately
				// have no input bytes; protobuf JSON may omit that default value.
				if input := value.Get("data"); input.Exists() {
					if input.Type != gjson.String {
						return fmt.Errorf("invalid TRON contract input")
					}
					if _, err := hex.DecodeString(input.String()); err != nil {
						return fmt.Errorf("invalid TRON contract input")
					}
				}
			}
		}
	}
	return troncodec.Verify(block)
}
