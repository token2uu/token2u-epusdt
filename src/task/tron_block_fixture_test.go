package task

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// sealTronTestBlock creates commitments for small TransferContract fixtures.
// The independent, captured mainnet fixture tests the production codec itself.
func sealTronTestBlock(t *testing.T, block map[string]interface{}) {
	t.Helper()
	bytesField := func(dst []byte, tag protowire.Number, value []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(dst, tag, protowire.BytesType), value)
	}
	intField := func(dst []byte, tag protowire.Number, value uint64) []byte {
		return protowire.AppendVarint(protowire.AppendTag(dst, tag, protowire.VarintType), value)
	}
	integer := func(value interface{}) uint64 {
		raw, _ := json.Marshal(value)
		var number uint64
		if err := json.Unmarshal(raw, &number); err != nil {
			t.Fatal(err)
		}
		return number
	}
	var leaves [][32]byte
	txs, _ := block["transactions"].([]interface{})
	for _, entry := range txs {
		tx := entry.(map[string]interface{})
		contract := tx["raw_data"].(map[string]interface{})["contract"].([]interface{})[0].(map[string]interface{})
		if contract["type"] != "TransferContract" {
			t.Fatal("fixture only supports TRX transfers")
		}
		value := contract["parameter"].(map[string]interface{})["value"].(map[string]interface{})
		var payload []byte
		for i, name := range []string{"owner_address", "to_address"} {
			decoded, err := hex.DecodeString(value[name].(string))
			if err != nil {
				t.Fatal(err)
			}
			payload = bytesField(payload, protowire.Number(i+1), decoded)
		}
		payload = intField(payload, 3, integer(value["amount"]))
		anyValue := bytesField(nil, 1, []byte("type.googleapis.com/protocol.TransferContract"))
		anyValue = bytesField(anyValue, 2, payload)
		raw := bytesField(nil, 11, bytesField(intField(nil, 1, 1), 2, anyValue))
		tx["raw_data_hex"] = hex.EncodeToString(raw)
		id := sha256.Sum256(raw)
		tx["txID"] = hex.EncodeToString(id[:])
		encoded := bytesField(bytesField(nil, 1, raw), 5, intField(nil, 3, 1))
		leaves = append(leaves, sha256.Sum256(encoded))
	}
	var root [32]byte
	for len(leaves) > 1 {
		var next [][32]byte
		for i := 0; i < len(leaves); i += 2 {
			if i+1 == len(leaves) {
				next = append(next, leaves[i])
				continue
			}
			pair := append(append([]byte{}, leaves[i][:]...), leaves[i+1][:]...)
			next = append(next, sha256.Sum256(pair))
		}
		leaves = next
	}
	if len(leaves) == 1 {
		root = leaves[0]
	}
	header := block["block_header"].(map[string]interface{})["raw_data"].(map[string]interface{})
	header["txTrieRoot"] = hex.EncodeToString(root[:])
	height := integer(header["number"])
	rawHeader := intField(bytesField(intField(nil, 1, integer(header["timestamp"])), 2, root[:]), 7, height)
	id := sha256.Sum256(rawHeader)
	binary.BigEndian.PutUint64(id[:8], height)
	block["blockID"] = hex.EncodeToString(id[:])
}
