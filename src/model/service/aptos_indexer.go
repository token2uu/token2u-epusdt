package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

const aptosIndexerMetadata = `ledger_infos { chain_id }
 processor_status(where: {processor: {_eq: $processor}}) { last_success_version }`

const aptosIndexerHeadQuery = `query PaymentHead($processor: String!) { ` + aptosIndexerMetadata + ` }`

const aptosIndexerDepositQuery = `query PaymentDeposits($processor: String!, $owner: String!, $version: bigint!, $event: bigint!, $head: bigint!, $limit: Int!) {
 ` + aptosIndexerMetadata + `
 fungible_asset_activities(
   where: {owner_address: {_eq: $owner}, type: {_eq: "0x1::fungible_asset::Deposit"}, is_gas_fee: {_eq: false}, is_transaction_success: {_eq: true},
     _and: [{transaction_version: {_lte: $head}}, {_or: [
       {transaction_version: {_gt: $version}},
       {transaction_version: {_eq: $version}, event_index: {_gt: $event}}
     ]}]},
   order_by: [{transaction_version: asc}, {event_index: asc}], limit: $limit
 ) { transaction_version event_index owner_address asset_type type amount }
}`

type AptosEventPosition struct {
	Version int64 `json:"version"`
	Index   int64 `json:"index"`
}

func (p AptosEventPosition) After(other AptosEventPosition) bool {
	return p.Version > other.Version || (p.Version == other.Version && p.Index > other.Index)
}

type AptosIndexedDeposit struct {
	Version json.Number `json:"transaction_version"`
	Index   json.Number `json:"event_index"`
	Owner   string      `json:"owner_address"`
	Asset   string      `json:"asset_type"`
	Type    string      `json:"type"`
	Amount  json.Number `json:"amount"`
}

func (d AptosIndexedDeposit) Position() (AptosEventPosition, error) {
	version, err := strconv.ParseInt(d.Version.String(), 10, 64)
	if err != nil || version < 0 {
		return AptosEventPosition{}, fmt.Errorf("invalid indexed transaction version")
	}
	index, err := strconv.ParseInt(d.Index.String(), 10, 64)
	if err != nil || index < 0 {
		return AptosEventPosition{}, fmt.Errorf("invalid indexed event index")
	}
	return AptosEventPosition{Version: version, Index: index}, nil
}

type aptosIndexerResponse struct {
	requestStarted time.Time
	Ledger         []struct {
		ChainID *int `json:"chain_id"`
	} `json:"ledger_infos"`
	Processors []struct {
		Version json.Number `json:"last_success_version"`
	} `json:"processor_status"`
	Deposits []AptosIndexedDeposit `json:"fungible_asset_activities"`
}

func (r aptosIndexerResponse) head() (int64, error) {
	if len(r.Ledger) != 1 || r.Ledger[0].ChainID == nil || *r.Ledger[0].ChainID != 1 {
		return 0, fmt.Errorf("Aptos indexer is not verified as mainnet")
	}
	if len(r.Processors) != 1 {
		return 0, fmt.Errorf("Aptos indexer fungible asset processor status missing")
	}
	version, err := strconv.ParseInt(r.Processors[0].Version.String(), 10, 64)
	if err != nil || version < 0 {
		return 0, fmt.Errorf("invalid Aptos indexer processor version")
	}
	return version, nil
}

// Keep all indexer reads within the anonymous endpoint's shared budget.
var aptosIndexerLimiter = rate.NewLimiter(2, 1)
var aptosIndexerHTTPClient = &http.Client{Timeout: 20 * time.Second}

func aptosIndexerNodes() ([]mdb.RpcNode, error) {
	nodes, err := data.ListGeneralRpcCandidates(mdb.NetworkAptos, mdb.RpcNodeTypeIndexer)
	if err != nil {
		return nil, err
	}
	if len(nodes) > 0 {
		return nodes, nil
	}
	// Explicit database configuration owns the pool, including disabled/down rows.
	configured, err := data.ListRpcNodes(mdb.NetworkAptos)
	if err != nil {
		return nil, err
	}
	for _, node := range configured {
		if node.Type == mdb.RpcNodeTypeIndexer {
			return nil, fmt.Errorf("no available Aptos indexer configured")
		}
	}
	endpoint := strings.TrimSpace(viper.GetString("aptos_indexer_url"))
	if endpoint == "" {
		endpoint = "https://api.mainnet.aptoslabs.com/v1/graphql"
	}
	nodes = []mdb.RpcNode{{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: endpoint, ApiKey: strings.TrimSpace(viper.GetString("aptos_indexer_api_key")), Purpose: mdb.RpcNodePurposeGeneral}}
	fallbackKey := strings.TrimSpace(viper.GetString("aptos_indexer_fallback_api_key"))
	if fallback := strings.TrimSpace(viper.GetString("aptos_indexer_fallback_url")); fallback != "" && (fallback != endpoint || fallbackKey != nodes[0].ApiKey) {
		nodes = append(nodes, mdb.RpcNode{Network: mdb.NetworkAptos, Type: mdb.RpcNodeTypeIndexer, Url: fallback, ApiKey: fallbackKey, Purpose: mdb.RpcNodePurposePaidFallback})
	}
	return nodes, nil
}

func aptosIndexerQuery(ctx context.Context, query string, variables map[string]interface{}, validate func(aptosIndexerResponse) error) (aptosIndexerResponse, error) {
	var result aptosIndexerResponse
	nodes, err := aptosIndexerNodes()
	if err != nil {
		return result, err
	}
	if IsPaidPaymentReview(ctx) {
		paid := nodes[:0]
		for _, node := range nodes {
			if node.Purpose == mdb.RpcNodePurposePaidFallback {
				paid = append(paid, node)
			}
		}
		nodes = paid
		if len(nodes) == 0 {
			return result, fmt.Errorf("no paid Aptos indexer configured for payment review")
		}
	}
	processor := strings.TrimSpace(viper.GetString("aptos_indexer_processor"))
	if processor == "" {
		processor = "fungible_asset_processor"
	}
	variables["processor"] = processor
	body, err := json.Marshal(map[string]interface{}{"query": query, "variables": variables})
	if err != nil {
		return result, err
	}
	var lastErr error
	for attempts := 0; attempts < 3 && len(nodes) > 0; {
		index := 0
		// Reserve the last attempt for an available paid fallback. Without
		// one, a third public node can still recover the request.
		if attempts == 2 {
			for i, node := range nodes {
				if node.Purpose == mdb.RpcNodePurposePaidFallback && !data.RpcEndpointCoolingDown(node) {
					index = i
					break
				}
			}
		}
		node := nodes[index]
		nodes = append(nodes[:index], nodes[index+1:]...)
		if data.RpcEndpointCoolingDown(node) {
			continue
		}
		attempts++
		result, err = aptosIndexerRequest(ctx, node, body, RPCErrorReason(lastErr))
		if err == nil {
			err = validate(result)
		}
		if err == nil {
			head, headErr := result.head()
			if headErr != nil {
				err = headErr
			} else if !data.ObserveRpcHead(node, head, result.requestStarted) {
				err = &RPCRequestError{Reason: "stale_head"}
			}
		}
		if err == nil {
			data.RecordRpcNodeSuccess(node.ID)
			return result, nil
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		lastErr = err
		if RPCErrorReason(err) == "local_capacity" {
			continue
		}
		data.RecordRpcNodeFailure(node.ID)
		if RPCErrorReason(err) != "rate_limited" {
			data.CooldownRpcEndpoint(node, "")
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no available Aptos indexer; endpoints cooling down")
	}
	return aptosIndexerResponse{}, lastErr
}

func aptosIndexerRequest(ctx context.Context, node mdb.RpcNode, body []byte, reason string) (aptosIndexerResponse, error) {
	var result aptosIndexerResponse
	u, err := url.Parse(node.Url)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return result, fmt.Errorf("invalid Aptos indexer URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, node.Url, bytes.NewReader(body))
	if err != nil {
		return result, fmt.Errorf("create Aptos indexer request")
	}
	req.Header.Set("Content-Type", "application/json")
	if node.ApiKey != "" {
		req.Header.Set("Authorization", "Bearer "+node.ApiKey)
	}
	client := *aptosIndexerHTTPClient
	client.Timeout = RPCRequestTimeout(node, client.Timeout)
	client.Transport = NewRPCTransport(node, reason, client.Transport)
	resp, err := client.Do(req)
	if err != nil {
		return result, WrapRPCError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, rpcStatusError(resp.StatusCode)
	}
	var envelope struct {
		Data   *aptosIndexerResponse `json:"data"`
		Errors []json.RawMessage     `json:"errors"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&envelope); err != nil {
		return result, fmt.Errorf("invalid Aptos indexer JSON")
	}
	if len(envelope.Errors) != 0 || envelope.Data == nil {
		return result, fmt.Errorf("Aptos indexer returned GraphQL errors or missing data")
	}
	envelope.Data.requestStarted = RPCRequestStartedAt(resp)
	return *envelope.Data, nil
}

func AptosIndexerHead(ctx context.Context) (int64, error) {
	result, err := aptosIndexerQuery(ctx, aptosIndexerHeadQuery, map[string]interface{}{}, func(result aptosIndexerResponse) error {
		_, err := result.head()
		return err
	})
	if err != nil {
		return 0, err
	}
	return result.head()
}

func AptosIndexerDeposits(ctx context.Context, owner string, after AptosEventPosition, head int64, limit int) ([]AptosIndexedDeposit, error) {
	return AptosIndexerDepositsSince(ctx, owner, after, head, limit, "")
}

// An optional frozen timestamp bounds initial discovery without dropping
// deposits that arrived while the first indexer request was unavailable.
func AptosIndexerDepositsSince(ctx context.Context, owner string, after AptosEventPosition, head int64, limit int, since string) ([]AptosIndexedDeposit, error) {
	query := aptosIndexerDepositQuery
	variables := map[string]interface{}{
		"owner": owner, "version": strconv.FormatInt(after.Version, 10), "event": strconv.FormatInt(after.Index, 10), "head": strconv.FormatInt(head, 10), "limit": limit,
	}
	if since != "" {
		boundary, err := time.Parse(time.RFC3339Nano, since)
		if err != nil {
			return nil, fmt.Errorf("invalid Aptos scan start time")
		}
		query = strings.Replace(query, "$limit: Int!", "$limit: Int!, $since: timestamp!", 1)
		query = strings.Replace(query, "where: {owner_address:", "where: {transaction_timestamp: {_gte: $since}, owner_address:", 1)
		variables["since"] = boundary.UTC().Format(time.RFC3339Nano)
	}
	result, err := aptosIndexerQuery(ctx, query, variables, func(result aptosIndexerResponse) error {
		indexed, err := result.head()
		if err != nil {
			return err
		}
		if indexed < head {
			return &RPCRequestError{Reason: "stale_head"}
		}
		return ValidateAptosDepositPage(result.Deposits, owner, after, head, limit)
	})
	return result.Deposits, err
}

func ValidateAptosDepositPage(rows []AptosIndexedDeposit, owner string, after AptosEventPosition, head int64, limit int) error {
	if rows == nil || len(rows) > limit {
		return fmt.Errorf("missing or oversized Aptos deposit page")
	}
	canonicalOwner, err := addressutil.NormalizeMoveAddress(owner)
	if err != nil {
		return err
	}
	for _, row := range rows {
		pos, err := row.Position()
		if err != nil || !pos.After(after) || pos.Version > head {
			return fmt.Errorf("invalid or unordered Aptos deposit position")
		}
		address, err := addressutil.NormalizeMoveAddress(row.Owner)
		_, assetErr := addressutil.NormalizeMoveAddress(row.Asset)
		amount, ok := new(big.Int).SetString(row.Amount.String(), 10)
		if err != nil || address != canonicalOwner || row.Type != "0x1::fungible_asset::Deposit" || assetErr != nil || !ok || amount.Sign() < 0 {
			return fmt.Errorf("invalid Aptos indexed deposit")
		}
		after = pos
	}
	return nil
}
