package troncodec

import (
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/tidwall/gjson"
)

// Captured from the public TRON mainnet API, block 87034506 (304 transactions).
// Its chain-supplied roots are independent golden values, not generated here.
func mainnetFixture(t *testing.T) []byte {
	t.Helper()
	f, e := os.Open("testdata/block-87034506.json.gz")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	z, e := gzip.NewReader(f)
	if e != nil {
		t.Fatal(e)
	}
	defer z.Close()
	raw, e := io.ReadAll(z)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
func TestMainnetCommitment(t *testing.T) {
	raw := mainnetFixture(t)
	if err := Verify(gjson.ParseBytes(raw)); err != nil {
		t.Fatal(err)
	}
}

func TestResultUnfreezeMapWireCompatibility(t *testing.T) {
	// Canonical protobuf: SUCCESS (field 3), then two map entries (field 28).
	// The HTTP array form comes from java-tron's JsonFormat; some providers
	// return standard protobuf-JSON objects instead. Entry order must survive.
	want := "1801e2010d0a0942414e4457494454481005e2010a0a06454e455247591000"
	for _, raw := range []string{
		`{"contractRet":"SUCCESS","cancel_unfreezeV2_amount":[{"key":"BANDWIDTH","value":5},{"key":"ENERGY"}]}`,
		`{"contractRet":"SUCCESS","cancel_unfreezeV2_amount":{"BANDWIDTH":5,"ENERGY":0}}`,
	} {
		got, err := encodeResult(gjson.Parse(raw))
		if err != nil || hex.EncodeToString(got) != want {
			t.Fatalf("unfreeze result encoding=%x err=%v", got, err)
		}
	}
}
func TestRejectMutatedCommitments(t *testing.T) {
	for _, kind := range []string{"remove_transaction", "reorder_transactions", "amount", "signature", "result", "header"} {
		t.Run(kind, func(t *testing.T) {
			var block map[string]interface{}
			if err := json.Unmarshal(mainnetFixture(t), &block); err != nil {
				t.Fatal(err)
			}
			txs := block["transactions"].([]interface{})
			first := txs[0].(map[string]interface{})
			switch kind {
			case "remove_transaction":
				block["transactions"] = txs[1:]
			case "reorder_transactions":
				txs[0], txs[1] = txs[1], txs[0]
			case "signature":
				first["signature"] = []string{}
			case "result":
				first["ret"] = []interface{}{map[string]interface{}{"contractRet": "REVERT"}}
			case "header":
				block["block_header"].(map[string]interface{})["raw_data"].(map[string]interface{})["timestamp"] = float64(1)
			case "amount":
				for _, v := range txs {
					tx := v.(map[string]interface{})
					c := tx["raw_data"].(map[string]interface{})["contract"].([]interface{})[0].(map[string]interface{})
					if c["type"] == "TransferContract" {
						value := c["parameter"].(map[string]interface{})["value"].(map[string]interface{})
						value["amount"] = value["amount"].(float64) + 1
						break
					}
				}
			}
			raw, e := json.Marshal(block)
			if e != nil {
				t.Fatal(e)
			}
			if err := Verify(gjson.ParseBytes(raw)); err == nil {
				t.Fatal("mutated commitment accepted")
			}
		})
	}
}
