package data

import (
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func TestPoolHeadFluctuationDoesNotHideStall(t *testing.T) {
	ResetRpcEndpointPolicyForTest()
	t.Cleanup(ResetRpcEndpointPolicyForTest)
	previous := viper.Get("rpc_stale_head_seconds")
	viper.Set("rpc_stale_head_seconds", 120)
	t.Cleanup(func() { viper.Set("rpc_stale_head_seconds", previous) })
	now := time.Now()
	rpcEndpointPolicy.now = func() time.Time { return now }
	node := mdb.RpcNode{Network: mdb.NetworkTon, Type: mdb.RpcNodeTypeLite, Url: "https://pool.test/global.config.json"}
	if !ObserveRpcPoolHead(node, 100) {
		t.Fatal("initial height rejected")
	}
	for _, head := range []int64{99, 100, 98, 100} {
		now = now.Add(20 * time.Second)
		if !ObserveRpcPoolHead(node, head) {
			t.Fatalf("short pool fluctuation rejected: %d", head)
		}
	}
	now = now.Add(41 * time.Second)
	if ObserveRpcPoolHead(node, 99) || ObserveRpcPoolHead(node, 100) {
		t.Fatal("oscillating old heads reset the stall timer")
	}
	other := node
	other.Url = "https://other-pool.test/global.config.json"
	if !ObserveRpcPoolHead(other, 100) {
		t.Fatal("one pool's stall poisoned another pool")
	}
	if !ObserveRpcPoolHead(node, 101) {
		t.Fatal("new highest head did not recover the pool")
	}
	// The existing single-endpoint rule remains strict for a later request.
	strict := mdb.RpcNode{Network: mdb.NetworkEthereum, Type: mdb.RpcNodeTypeHttp, Url: "https://single.test"}
	if !ObserveRpcHead(strict, 100, now) {
		t.Fatal("initial single-endpoint height rejected")
	}
	now = now.Add(time.Second)
	if ObserveRpcHead(strict, 99, now) {
		t.Fatal("pool tolerance leaked into the single-endpoint policy")
	}
}
