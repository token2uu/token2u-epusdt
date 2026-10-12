package task

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestRPCHealthChecksProtocolAndLimitsPaidProbes(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	resetPaidFallbackState(t)
	for _, tc := range []struct {
		name, body string
		status     int
		want       string
	}{
		{"valid", `{"jsonrpc":"2.0","result":100}`, 200, mdb.RpcNodeStatusOk},
		{"rpc_error", `{"jsonrpc":"2.0","error":{"code":-32005}}`, 200, mdb.RpcNodeStatusDown},
		{"throttled", `{}`, 429, mdb.RpcNodeStatusDown},
		{"not_rpc", `{}`, 200, mdb.RpcNodeStatusDown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			status, _, err := ProbeConfiguredRpcNode(mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			if status != tc.want {
				t.Fatalf("TCP reachable but protocol status=%s want=%s", status, tc.want)
			}
		})
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":101}`))
	}))
	defer server.Close()
	if err := dao.Mdb.Create(&mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Purpose: mdb.RpcNodePurposePaidFallback, Enabled: true, Status: mdb.RpcNodeStatusUnknown}).Error; err != nil {
		t.Fatal(err)
	}
	RpcHealthJob{}.Run()
	RpcHealthJob{}.Run()
	if calls != 1 {
		t.Fatalf("paid health probes not throttled: %d", calls)
	}
	if data.SnapshotRpcRoutingStats()[mdb.NetworkSolana].PaidRequests != 1 {
		t.Fatal("paid probe missing from usage counters")
	}
}
