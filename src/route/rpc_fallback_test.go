package route

import (
	"fmt"
	"testing"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestAdminConfiguresPaidFallbackAndIndexer(t *testing.T) {
	e, token := setupAdminTestEnv(t)
	for _, typ := range []string{mdb.RpcNodeTypeHttp, mdb.RpcNodeTypeIndexer} {
		rec := doPostAdmin(e, "/admin/api/v1/rpc-nodes", map[string]interface{}{"network": mdb.NetworkAptos, "type": typ, "url": "https://rpc.example.test/endpoint", "purpose": mdb.RpcNodePurposePaidFallback}, token)
		response := assertOK(t, rec)
		payload := response["data"].(map[string]interface{})
		id := uint64(payload["id"].(float64))
		node, err := data.GetRpcNodeByID(id)
		if err != nil || node.Type != typ || node.Purpose != mdb.RpcNodePurposePaidFallback {
			t.Fatalf("lost fallback configuration: %+v %v", node, err)
		}
		assertOK(t, doPatchAdmin(e, fmt.Sprintf("/admin/api/v1/rpc-nodes/%d", id), map[string]interface{}{"purpose": mdb.RpcNodePurposeGeneral}, token))
		node, err = data.GetRpcNodeByID(id)
		if err != nil || node.Purpose != mdb.RpcNodePurposeGeneral {
			t.Fatalf("cannot restore public priority: %+v %v", node, err)
		}
	}
	rec := doPostAdmin(e, "/admin/api/v1/rpc-nodes", map[string]interface{}{"network": mdb.NetworkTron, "type": mdb.RpcNodeTypeIndexer, "url": "https://rpc.example.test"}, token)
	assertErrorCode(t, rec, 10024)
}
