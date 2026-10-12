package task

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTronEmptyBlockRequiresZeroTransactionRoot(t *testing.T) {
	empty := map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": 2, "timestamp": 1000}}}
	sealTronTestBlock(t, empty)
	id, _ := json.Marshal(empty["blockID"])
	for _, tc := range []struct {
		name, root, transactions string
		valid                    bool
	}{
		{"omitted_empty_list", strings.Repeat("0", 64), "", true},
		{"explicit_empty_list", strings.Repeat("0", 64), `,"transactions":[]`, true},
		{"omitted_committed_list", strings.Repeat("1", 64), "", false},
		{"empty_committed_list", strings.Repeat("1", 64), `,"transactions":[]`, false},
		{"null_list", strings.Repeat("0", 64), `,"transactions":null`, false},
		{"missing_root", "", "", false},
		{"malformed_transaction", strings.Repeat("1", 64), `,"transactions":[{}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"blockID":` + string(id) + `,"block_header":{"raw_data":{"number":2,"timestamp":1000,"txTrieRoot":"` + tc.root + `"}}` + tc.transactions + `}`)
			if err := validateTronBlockResponse(body, nil); (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
		})
	}
}
