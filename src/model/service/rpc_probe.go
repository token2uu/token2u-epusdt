package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
)

// Local admission failure is not evidence that the remote node is unhealthy.
var ErrRPCProbeDeferred = errors.New("RPC probe deferred by local request budget")

// ProbeRPCNode reads a real, inexpensive chain/indexer head. TCP connectivity
// alone cannot detect bad credentials, 429 responses or a stalled provider.
func ProbeRPCNode(node mdb.RpcNode) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return probeRPCNode(ctx, node)
}

func probeRPCNode(ctx context.Context, node mdb.RpcNode) error {
	method, endpoint := http.MethodPost, strings.TrimRight(node.Url, "/")
	var payload interface{}
	switch {
	case node.Type == mdb.RpcNodeTypeIndexer && node.Network == mdb.NetworkAptos:
		processor := strings.TrimSpace(viper.GetString("aptos_indexer_processor"))
		if processor == "" {
			processor = "fungible_asset_processor"
		}
		payload = map[string]interface{}{"query": aptosIndexerHeadQuery, "variables": map[string]string{"processor": processor}}
	case node.Network == mdb.NetworkSolana:
		payload = map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": "getBlockHeight", "params": []interface{}{map[string]string{"commitment": "finalized"}}}
	case node.Network == mdb.NetworkTron:
		endpoint += "/wallet/getnowblock"
		payload = map[string]interface{}{}
	case node.Network == mdb.NetworkAptos:
		var err error
		endpoint, err = aptosJoinURL(node.Url, "/v1")
		if err != nil {
			return err
		}
		method = http.MethodGet
	default:
		return fmt.Errorf("unsupported RPC probe network")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("invalid probe URL")
	}
	req.Header.Set("Content-Type", "application/json")
	if node.ApiKey != "" {
		if node.Network == mdb.NetworkTron {
			req.Header.Set("TRON-PRO-API-KEY", node.ApiKey)
		} else {
			req.Header.Set("Authorization", "Bearer "+node.ApiKey)
		}
	}
	resp, err := rpcHTTPClient(node, "", 5*time.Second).Do(req)
	if err != nil {
		if RPCErrorReason(err) == "local_capacity" {
			return ErrRPCProbeDeferred
		}
		return fmt.Errorf("RPC probe failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return rpcStatusError(resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil || !gjson.ValidBytes(raw) {
		return fmt.Errorf("invalid probe response")
	}
	var head int64
	switch {
	case node.Type == mdb.RpcNodeTypeIndexer:
		var envelope struct {
			Data   *aptosIndexerResponse `json:"data"`
			Errors []json.RawMessage     `json:"errors"`
		}
		if json.Unmarshal(raw, &envelope) != nil || envelope.Data == nil || len(envelope.Errors) > 0 {
			return fmt.Errorf("invalid indexer probe")
		}
		head, err = envelope.Data.head()
		if err != nil {
			return err
		}
	case node.Network == mdb.NetworkSolana:
		if gjson.GetBytes(raw, "error").Exists() && gjson.GetBytes(raw, "error").Type != gjson.Null {
			return fmt.Errorf("Solana probe RPC error")
		}
		head = gjson.GetBytes(raw, "result").Int()
	case node.Network == mdb.NetworkTron:
		if gjson.GetBytes(raw, "blockID").String() == "" || gjson.GetBytes(raw, "block_header.raw_data.timestamp").Int() <= 0 {
			return fmt.Errorf("incomplete TRON probe")
		}
		head = gjson.GetBytes(raw, "block_header.raw_data.number").Int()
	case node.Network == mdb.NetworkAptos:
		if gjson.GetBytes(raw, "chain_id").Int() != 1 {
			return fmt.Errorf("Aptos probe is not mainnet")
		}
		head = gjson.GetBytes(raw, "ledger_version").Int()
	}
	if head <= 0 {
		return fmt.Errorf("invalid probe head")
	}
	if !data.ObserveRpcHead(node, head, RPCRequestStartedAt(resp)) {
		return &RPCRequestError{Reason: "stale_head"}
	}
	return nil
}
