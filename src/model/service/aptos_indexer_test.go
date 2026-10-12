package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

func TestAptosIndexerQueriesAddressAndPreservesIntegerPrecision(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	oldLimiter := aptosIndexerLimiter
	aptosIndexerLimiter = rate.NewLimiter(rate.Inf, 1)
	t.Cleanup(func() { aptosIndexerLimiter = oldLimiter; viper.Reset() })
	const head int64 = 9007199254740993
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("missing request method or authorization")
		}
		var req struct {
			Query     string
			Variables map[string]interface{}
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if req.Variables["processor"] != "fungible_asset_processor" {
			t.Error("missing exact processor filter")
		}
		data := map[string]interface{}{"ledger_infos": []interface{}{map[string]int{"chain_id": 1}}, "processor_status": []interface{}{map[string]int64{"last_success_version": head}}}
		if strings.Contains(req.Query, "PaymentDeposits") {
			if req.Variables["owner"] != "0xabc" || req.Variables["version"] != "9007199254740991" || req.Variables["event"] != "7" || req.Variables["head"] != "9007199254740993" || req.Variables["limit"] != float64(100) {
				t.Errorf("wrong query bounds: %+v", req.Variables)
			}
			if !strings.Contains(req.Query, "owner_address: {_eq: $owner}") || !strings.Contains(req.Query, "event_index: {_gt: $event}") {
				t.Error("query missing address filter or event pagination")
			}
			data["fungible_asset_activities"] = []interface{}{map[string]interface{}{"transaction_version": head, "event_index": 8, "owner_address": "0xabc", "asset_type": "0x1", "type": "0x1::fungible_asset::Deposit", "amount": "12345678901234567890"}}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": data})
	}))
	t.Cleanup(server.Close)
	viper.Set("aptos_indexer_url", server.URL)
	viper.Set("aptos_indexer_api_key", "test-key")
	got, err := AptosIndexerHead(context.Background())
	if err != nil || got != head {
		t.Fatalf("head=%d err=%v", got, err)
	}
	rows, err := AptosIndexerDeposits(context.Background(), "0xabc", AptosEventPosition{Version: 9007199254740991, Index: 7}, head, 100)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	pos, err := rows[0].Position()
	if err != nil || pos.Version != head || rows[0].Amount.String() != "12345678901234567890" || calls != 2 {
		t.Fatalf("precision lost: %+v %+v %v", pos, rows, err)
	}
}

func TestAptosIndexerRejectsPartialErrorsWrongNetworkAndLag(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	oldLimiter := aptosIndexerLimiter
	aptosIndexerLimiter = rate.NewLimiter(rate.Inf, 1)
	t.Cleanup(func() { aptosIndexerLimiter = oldLimiter; viper.Reset() })
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"partial_errors", `{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":100}],"fungible_asset_activities":[]},"errors":[{"message":"secret-key"}]}`, 200},
		{"wrong_network", `{"data":{"ledger_infos":[{"chain_id":2}],"processor_status":[{"last_success_version":100}],"fungible_asset_activities":[]}}`, 200},
		{"no_processor", `{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[],"fungible_asset_activities":[]}}`, 200},
		{"missing_deposits", `{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":100}]}}`, 200},
		{"lag", `{"data":{"ledger_infos":[{"chain_id":1}],"processor_status":[{"last_success_version":99}],"fungible_asset_activities":[]}}`, 200},
		{"throttle", `secret-key`, 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			viper.Set("aptos_indexer_url", server.URL+"/secret-key")
			_, err := AptosIndexerDeposits(context.Background(), "0xabc", AptosEventPosition{}, 100, 100)
			if err == nil || strings.Contains(err.Error(), "secret-key") {
				t.Fatalf("unsafe response accepted/leaked: %v", err)
			}
		})
	}
}
