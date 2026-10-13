package service

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/GMWalletApp/epusdt/util/math"
	"github.com/gagliardetto/solana-go"
	"github.com/go-resty/resty/v2"
	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
)

// gProcessedSignatures 已处理签名缓存，避免重复调用 getTransaction
var gProcessedSignatures sync.Map // sig -> unix timestamp

var processSolanaOrder = OrderProcessing

var (
	solRPCRetryCount   = 5
	solRPCRetryWait    = 2 * time.Second
	solRPCRetryMaxWait = 10 * time.Second
)

type TransferInfo struct {
	Source      string  // Source address (for SOL) or source ATA (for SPL tokens)
	Destination string  // Destination address (for SOL) or destination ATA (for SPL tokens)
	Mint        string  // Token mint (e.g. USDT mint, USDC mint, or "SOL" for native transfers)
	Amount      float64 // Human-readable amount (adjusted for decimals)
	RawAmount   uint64  // Raw amount from the transaction (before adjusting for decimals)
	Decimals    *int    // Optional decimals from the transaction, if available
	BlockTime   int64   // Block time of the transfer unit is seconds since epoch
}

// SolCallBack 扫描指定钱包地址的 Solana 链上交易，匹配待支付订单并确认收款。
func SolCallBack(address string, wg *sync.WaitGroup) {
	defer wg.Done()
	_ = ScanSolanaAddress(address)
}

// ScanSolanaAddress reports completion so one healthy wallet cannot hide a
// failed wallet in the scheduler's overall scan-progress statistics.
func ScanSolanaAddress(address string) (scanErr error) {
	return ScanSolanaAddressWithContext(context.Background(), address)
}

func ScanSolanaAddressWithContext(ctx context.Context, address string) (scanErr error) {
	if IsPaidPaymentReview(ctx) {
		if _, loaded := solReviewsInFlight.LoadOrStore(address, struct{}{}); loaded {
			return fmt.Errorf("Solana payment review already running for %s", address)
		}
		defer solReviewsInFlight.Delete(address)
	}
	scanErr = fmt.Errorf("Solana address scan incomplete")
	defer func() {
		if err := recover(); err != nil {
			log.Sugar.Errorf("[SOL][%s] panic recovered: %v", address, err)
		}
	}()

	// Load enabled Solana tokens from chain_tokens. The contract_address
	// column holds the mint for SPL tokens; a special entry with
	// symbol=SOL and empty contract_address enables native SOL payments.
	tokens, err := data.ListEnabledChainTokensByNetwork(mdb.NetworkSolana)
	if err != nil {
		log.Sugar.Errorf("[SOL][%s] load chain_tokens err=%v", address, err)
		return
	}
	if len(tokens) == 0 {
		log.Sugar.Debugf("[SOL][%s] no enabled chain_tokens, skipping", address)
		scanErr = nil
		return
	}
	mintTokens := make(map[string]*mdb.ChainToken, len(tokens))
	var nativeSolToken *mdb.ChainToken
	for i := range tokens {
		sym := strings.ToUpper(strings.TrimSpace(tokens[i].Symbol))
		mint := strings.TrimSpace(tokens[i].ContractAddress)
		if sym == "SOL" && mint == "" {
			nativeSolToken = &tokens[i]
			continue
		}
		if mint == "" {
			continue
		}
		mintTokens[mint] = &tokens[i]
	}

	// Clean up old entries from processed cache (older than 1 hour)
	cleanupCutoff := time.Now().Add(-1 * time.Hour).Unix()
	gProcessedSignatures.Range(func(key, value interface{}) bool {
		if ts, ok := value.(int64); ok && ts < cleanupCutoff {
			gProcessedSignatures.Delete(key)
		}
		return true
	})

	// 查询钱包地址 + 每个已启用 SPL 代币 ATA 的签名
	queryAddrs := []string{address}
	for mint := range mintTokens {
		ata, err := FindATAAddress(address, mint)
		if err != nil {
			log.Sugar.Errorf("[SOL][%s] failed to derive ATA for mint %s: %v", address, mint, err)
			continue
		}
		queryAddrs = append(queryAddrs, ata)
	}

	// Each watched owner/ATA has a durable high-water signature. Advance all
	// cursors only after every fetched transaction is durably processed.
	cutoffTime := time.Now().Add(-config.GetOrderExpirationTimeDuration() - 5*time.Minute).Unix()
	initialBoundaryLoaded := false
	queries := make([]solSignatureQuery, 0, len(queryAddrs))
	allProcessed := true
	for _, queryAddr := range queryAddrs {
		key := "solana:signatures:" + queryAddr
		if IsPaidPaymentReview(ctx) {
			key = "solana:payment_review:" + queryAddr
		}
		until, err := data.LoadPaymentCursor(key)
		if err != nil {
			log.Sugar.Errorf("[SOL] load cursor: %v", err)
			return
		}
		queryCutoffTime := cutoffTime
		if until == "" {
			// Freeze the initial lookback while retrying. Otherwise a payment
			// with incomplete details can age out before the first cursor commits.
			startKey := key + ":start_time"
			start, err := data.LoadPaymentCursor(startKey)
			if err != nil {
				log.Sugar.Errorf("[SOL] load initial scan boundary: %v", err)
				return
			}
			if start == "" {
				if !initialBoundaryLoaded {
					boundary, err := data.PaymentScanStartTime(mdb.NetworkSolana, address, time.Now())
					if err != nil {
						log.Sugar.Errorf("[SOL] determine initial scan boundary: %v", err)
						return
					}
					cutoffTime = boundary.Unix()
					initialBoundaryLoaded = true
				}
				queryCutoffTime = cutoffTime
				if err := data.SavePaymentCursor(startKey, strconv.FormatInt(queryCutoffTime, 10)); err != nil {
					log.Sugar.Errorf("[SOL] save initial scan boundary: %v", err)
					return
				}
			} else {
				queryCutoffTime, err = strconv.ParseInt(start, 10, 64)
				if err != nil || queryCutoffTime <= 0 {
					log.Sugar.Errorf("[SOL] invalid initial scan boundary for %s: %q", queryAddr, start)
					return
				}
			}
		}
		queries = append(queries, solSignatureQuery{Address: queryAddr, Key: key, Until: until, Cutoff: queryCutoffTime})
	}
	// Paid scans checkpoint each page and transaction, so the next bounded
	// round can resume even when the entire address history takes >12 seconds.
	work, err := collectSolanaSignatures(ctx, address, queries)
	if err != nil {
		log.Sugar.Errorf("[SOL] collect signatures owner=%s: %v", address, err)
		return
	}
	result := work.Signatures

	if len(result) == 0 {
		log.Sugar.Debugf("[SOL][%s] no transaction signatures found", address)
	}

	// Process oldest signatures first.
	// 按 blockTime 排列
	sort.Slice(result, func(i, j int) bool {
		if result[i].BlockTime == nil {
			return false
		}
		if result[j].BlockTime == nil {
			return true
		}
		return result[i].Slot < result[j].Slot
	})

	log.Sugar.Debugf("[SOL][%s] fetched %d unique signatures from %d addresses, cutoff=%d",
		address, len(result), len(queryAddrs), cutoffTime)

	// Process each transaction signature
	for sigIdx, txSig := range result {
		if ctx.Err() != nil {
			return
		}
		sig := txSig.Signature
		if work.Processed[sig] {
			continue
		}
		retrySignature := false

		// Skip failed transactions
		if txSig.Err != nil {
			continue
		}

		// 跳过已处理的签名
		if _, ok := gProcessedSignatures.Load(address + ":" + sig); ok && !IsPaidPaymentReview(ctx) {
			continue
		}

		log.Sugar.Debugf("[SOL][%s] [%d/%d] processing sig=%s slot=%d", address, sigIdx+1, len(result), sig, txSig.Slot)

		transfers, err := solGetTransactionTransfersWithContext(ctx, sig, queryAddrs, mintTokens)
		if err != nil {
			log.Sugar.Debugf("[SOL][%s] sig=%s fetch failed: %v", address, sig, err)
			allProcessed = false
			continue
		}

		for instrIdx, transferInfo := range transfers {
			if transferInfo == nil {
				continue
			}

			log.Sugar.Debugf("[SOL][%s] sig=%s instr[%d] transfer: src=%s dst=%s mint=%s rawAmount=%d amount=%.6f blockTime=%d",
				address, sig, instrIdx,
				transferInfo.Source, transferInfo.Destination, transferInfo.Mint,
				transferInfo.RawAmount, transferInfo.Amount, transferInfo.BlockTime)

			if !isTransferToAddress(transferInfo, address) {
				continue
			}

			cfg := mintTokens[transferInfo.Mint]
			assetID := transferInfo.Mint
			if transferInfo.Mint == "SOL" {
				cfg = nativeSolToken
				assetID = ""
			}
			symbol := ""
			decimals := 6
			if cfg != nil {
				symbol = cfg.Symbol
				decimals = cfg.Decimals
			}
			if cfg == nil && transferInfo.Decimals != nil {
				decimals = *transferInfo.Decimals
			}
			if transferInfo.Mint == "SOL" {
				decimals = 9
			}
			observed := data.ObservedPayment{Network: mdb.NetworkSolana, Address: address,
				Token: symbol, AssetID: assetID, RawAmount: new(big.Int).SetUint64(transferInfo.RawAmount), Decimals: decimals,
				TxHash: sig, EventID: fmt.Sprintf("instruction:%d", instrIdx), BlockTimeMs: transferInfo.BlockTime * 1000}
			handled, collectionErr := processObservedPayment(observed)
			if collectionErr != nil {
				retrySignature = true
				log.Sugar.Errorf("[SOL] collection: %v", collectionErr)
				continue
			}
			if handled {
				continue
			}
			token, amount := resolveSolTokenAndAmount(transferInfo, mintTokens, nativeSolToken)
			if token == "" || amount <= 0 {
				continue
			}

			log.Sugar.Infof("[SOL][%s] sig=%s instr[%d] incoming transfer confirmed: token=%s amount=%.2f -> querying transaction_lock network=solana address=%s token=%s amount=%.2f",
				address, sig, instrIdx, token, amount, address, token, amount)

			matchResult, err := data.GetTradeIdByWalletAddressAndAmountAndTokenWithEpayFallback(mdb.NetworkSolana, address, token, amount)
			if err != nil {
				log.Sugar.Errorf("[SOL][%s] sig=%s query transaction_lock failed: %v", address, sig, err)
				retrySignature = true
				continue
			}
			tradeID := matchResult.TradeId
			if tradeID == "" {
				if _, err := processObservedPayment(observed); err != nil {
					retrySignature = true
					log.Sugar.Errorf("[SOL] reservation recovery: %v", err)
				}
				log.Sugar.Infof("[SOL][%s] sig=%s no active transaction_lock matched: network=solana address=%s token=%s amount=%.2f (no order or expired)",
					address, sig, address, token, amount)
				continue
			}
			if matchResult.IsEpayFallback {
				log.Sugar.Infof("[SOL][%s] sig=%s epay fallback match trade_id=%s expected_amount!=%.2f",
					address, sig, tradeID, amount)
			}
			log.Sugar.Infof("[SOL][%s] transaction_lock matched: trade_id=%s sig=%s token=%s amount=%.2f",
				address, tradeID, sig, token, amount)

			order, err := data.GetOrderInfoByTradeId(tradeID)
			if err != nil {
				log.Sugar.Errorf("[SOL][%s] sig=%s load order failed for trade_id=%s: %v", address, sig, tradeID, err)
				retrySignature = true
				continue
			}
			log.Sugar.Infof("[SOL][%s] order loaded: trade_id=%s order_id=%s status=%d assigned_at_ms=%d",
				address, tradeID, order.OrderId, order.Status, data.PaymentAssignmentTimeMs(order))

			// blockTime 秒 → 毫秒，与订单创建时间对齐
			blockTimestamp := transferInfo.BlockTime * 1000
			createTime := data.PaymentAssignmentTimeMs(order)
			log.Sugar.Infof("[SOL][%s] time check: sig=%s block_time_ms=%d order_assigned_ms=%d diff_ms=%d",
				address, sig, blockTimestamp, createTime, blockTimestamp-createTime)
			if blockTimestamp < createTime {
				log.Sugar.Warnf("[SOL][%s] sig=%s skipped: block_time_ms=%d is %d ms before address assigned_ms=%d (transaction predates the order)",
					address, sig, blockTimestamp, createTime-blockTimestamp, createTime)
				continue
			}

			if blockTimestamp+999 >= data.PaymentDeadline(order).UnixMilli() {
				log.Sugar.Warnf("[SOL] skip payment outside order deadline trade_id=%s sig=%s", tradeID, sig)
				continue
			}

			req := &request.OrderProcessingRequest{
				ReceiveAddress:     address,
				Token:              token,
				Network:            mdb.NetworkSolana,
				TradeId:            tradeID,
				Amount:             amount,
				BlockTransactionId: sig,
			}
			if matchResult.IsEpayFallback {
				req.PaidAmount = amount
			}
			log.Sugar.Infof("[SOL][%s] calling OrderProcessing: trade_id=%s sig=%s token=%s amount=%.2f",
				address, tradeID, sig, token, amount)
			err = processSolanaOrder(req)
			if err != nil {
				if errors.Is(err, constant.OrderStatusConflict) {
					handled, recoveryErr := processObservedPayment(observed)
					if recoveryErr != nil || !handled {
						retrySignature = true
						log.Sugar.Errorf("[SOL] status conflict recovery: handled=%v err=%v", handled, recoveryErr)
					}
					continue
				}
				if errors.Is(err, constant.OrderBlockAlreadyProcess) {
					log.Sugar.Infof("[SOL][%s] sig=%s already resolved: trade_id=%s reason=%v", address, sig, tradeID, err)
					continue
				}
				log.Sugar.Errorf("[SOL][%s] sig=%s OrderProcessing failed for trade_id=%s: %v", address, sig, tradeID, err)
				retrySignature = true
				continue
			}

			log.Sugar.Infof("[SOL][%s] order marked paid: trade_id=%s sig=%s token=%s amount=%.2f, sending telegram notification",
				address, tradeID, sig, token, amount)
			sendPaymentNotification(order)
			log.Sugar.Infof("[SOL][%s] payment fully processed: trade_id=%s sig=%s", address, tradeID, sig)
		}

		if retrySignature {
			allProcessed = false
			log.Sugar.Debugf("[SOL][%s] sig=%s not marked processed because retryable processing failed", address, sig)
			continue
		}

		// 标记已处理
		gProcessedSignatures.Store(address+":"+sig, time.Now().Unix())
		work.Processed[sig] = true
		if err := work.save(ctx, address); err != nil {
			log.Sugar.Errorf("[SOL] save review progress: %v", err)
			return
		}
	}
	if allProcessed {
		for _, query := range work.Queries {
			if query.Next == "" {
				continue
			}
			if err := data.SavePaymentCursor(query.Key, query.Next); err != nil {
				log.Sugar.Errorf("[SOL] save cursor: %v", err)
				return
			}
		}
		if IsPaidPaymentReview(ctx) {
			if err := data.SavePaymentCursor(solReviewWorkKey(address), ""); err != nil {
				log.Sugar.Errorf("[SOL] clear completed review: %v", err)
				return
			}
		}
		scanErr = nil
	}
	return
}

// solSignatureResult getSignaturesForAddress 返回结构
type solSignatureResult struct {
	Signature string      `json:"signature"`
	Slot      uint64      `json:"slot"`
	Err       interface{} `json:"err"`
	BlockTime *int64      `json:"blockTime"`
}

func resolveSolanaRpcURL() (string, error) {
	node, err := resolveSolanaRpcNode()
	if err != nil {
		return "", err
	}
	rpcURL := strings.TrimSpace(node.Url)
	return rpcURL, nil
}

func resolveSolanaRpcNode(excludeIDs ...uint64) (*mdb.RpcNode, error) {
	node, err := data.SelectGeneralRpcNode(mdb.NetworkSolana, mdb.RpcNodeTypeHttp, excludeIDs...)
	if err != nil {
		return nil, err
	}
	if node == nil || node.ID == 0 {
		return nil, fmt.Errorf("no enabled %s %s RPC node configured in rpc_nodes", mdb.NetworkSolana, mdb.RpcNodeTypeHttp)
	}
	rpcURL := strings.TrimSpace(node.Url)
	if rpcURL == "" {
		return nil, fmt.Errorf("rpc_nodes id=%d has empty url", node.ID)
	}
	node.Url = rpcURL
	return node, nil
}

// SolRetryClient 发送 Solana JSON-RPC 请求，自动重试
func SolRetryClient(method string, params []interface{}) (body []byte, err error) {
	return solRetryClientValidated(method, params, nil)
}

// validate is read-only and runs before accepting each candidate response.
func solRetryClientValidated(method string, params []interface{}, validate func([]byte) error) (body []byte, err error) {
	return solRetryClientValidatedWithContext(context.Background(), method, params, validate)
}

func solRetryClientValidatedWithContext(ctx context.Context, method string, params []interface{}, validate func([]byte) error) (body []byte, err error) {
	defer func() {
		if err != nil {
			data.RecordRpcFailure(mdb.NetworkSolana)
			return
		}
		data.RecordRpcSuccess(mdb.NetworkSolana)
	}()

	tried := make([]uint64, 0, 3)
	var lastErr error
	for attempts := 0; attempts < 3; attempts++ {
		node, err := SelectRPCNode(ctx, mdb.NetworkSolana, mdb.RpcNodeTypeHttp, tried...)
		if err == nil && node == nil {
			err = fmt.Errorf("no enabled Solana RPC node")
		}
		if err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
		headers := make(http.Header)
		if node.ApiKey != "" {
			headers.Set("Authorization", "Bearer "+node.ApiKey)
		}
		body, err := solHistoryBackoff.doContext(ctx, node.Url, method, params, headers, func() ([]byte, error) {
			return solRequestWithNodeContext(ctx, *node, method, params, headers, RPCErrorReason(lastErr), true)
		})
		if err == nil && validate != nil {
			if validationErr := validate(body); validationErr != nil {
				err = &RPCRequestError{Reason: "invalid_response", cause: validationErr}
			}
		}
		var deferred *solanaHistoryBackoffError
		if len(tried) > 0 && (!errors.As(err, &deferred) || !deferred.cached) {
			log.Sugar.Warnf("[SOL] trying alternate RPC node method=%s node=%s", method, data.RpcNodeLogLabel(*node))
		}
		if err == nil {
			data.RecordRpcNodeSuccess(node.ID)
			return body, nil
		}

		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if RPCErrorReason(err) == "local_capacity" {
			tried = append(tried, node.ID)
			continue
		}
		if isSolanaHistoryUnavailable(err) {
			// A node may be healthy but unable to resolve this history cursor.
			// Retry the identical query elsewhere immediately, without letting
			// unrelated successful requests reset its request-level backoff.
			var historyErr *solanaHistoryBackoffError
			if errors.As(err, &historyErr) && historyErr.cached {
				log.Sugar.Debugf("[SOL] history query cooling down method=%s node=%s retry_at=%s, trying another node", method, data.RpcNodeLogLabel(*node), historyErr.until.UTC().Format(time.RFC3339))
			} else {
				log.Sugar.Warnf("[SOL] history unavailable method=%s node=%s err=%v, retaining cursor and trying another node", method, data.RpcNodeLogLabel(*node), err)
			}
			tried = append(tried, node.ID)
			continue
		}
		failures, cooling := data.RecordRpcNodeFailure(node.ID)
		if RPCErrorReason(err) != "rate_limited" {
			data.CooldownRpcEndpoint(*node, "")
		}
		nodeLabel := data.RpcNodeLogLabel(*node)
		var limited *solanaRateLimitError
		if errors.As(err, &limited) {
			log.Sugar.Warnf("[SOL] RPC rate limited method=%s node=%s retry_at=%s, trying another node", method, nodeLabel, limited.until.UTC().Format(time.RFC3339))
		} else if !cooling {
			log.Sugar.Warnf("[SOL] RPC node failed method=%s node=%s failures=%d/%d", method, nodeLabel, failures, data.RpcFailoverThreshold)
		} else {
			log.Sugar.Warnf("[SOL] RPC node reached fail threshold method=%s node=%s, trying another node", method, nodeLabel)
		}
		tried = append(tried, node.ID)
	}
	return nil, lastErr
}

func solRetryClientWithURL(rpcUrl string, method string, params []interface{}) ([]byte, error) {
	return solRetryClientWithURLHeaders(rpcUrl, method, params, nil)
}

func solRetryClientWithURLHeaders(rpcUrl string, method string, params []interface{}, headers http.Header) ([]byte, error) {
	return solRetryClientWithNodeHeaders(mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: rpcUrl}, method, params, headers)
}

func solRetryClientWithNodeHeaders(node mdb.RpcNode, method string, params []interface{}, headers http.Header) ([]byte, error) {
	node.Url = strings.TrimSpace(node.Url)
	// Authentication must also participate in the history backoff identity.
	// Keep the full node for transport accounting and paid/public rate limits.
	headers = headers.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	if node.ApiKey != "" && headers.Get("Authorization") == "" {
		headers.Set("Authorization", "Bearer "+node.ApiKey)
	}
	return solHistoryBackoff.do(node.Url, method, params, headers, func() ([]byte, error) {
		return solRequestWithNode(node, method, params, headers, "", false)
	})
}

func solRequestWithURLHeaders(rpcUrl string, method string, params []interface{}, headers http.Header) ([]byte, error) {
	return solRequestWithNode(mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: rpcUrl}, method, params, headers, "", false)
}

func solRequestWithNode(node mdb.RpcNode, method string, params []interface{}, headers http.Header, reason string, failover bool) ([]byte, error) {
	return solRequestWithNodeContext(context.Background(), node, method, params, headers, reason, failover)
}

func solRequestWithNodeContext(ctx context.Context, node mdb.RpcNode, method string, params []interface{}, headers http.Header, reason string, failover bool) ([]byte, error) {
	rpcUrl := node.Url
	// Direct/manual callers pass authorization in headers; use the same identity
	// as automatic calls for both transport and Solana-specific cooldowns.
	if authorization := headers.Get("Authorization"); authorization != "" {
		node.ApiKey = strings.TrimPrefix(authorization, "Bearer ")
	}
	cooldownKey := solThrottleIdentity(rpcUrl, node.ApiKey)
	client := resty.NewWithClient(rpcHTTPClient(node, reason, 20*time.Second))
	client.SetRetryCount(solRPCRetryCount)
	if failover {
		client.SetRetryCount(0)
	}
	client.SetRetryWaitTime(solRPCRetryWait)
	client.SetRetryMaxWaitTime(solRPCRetryMaxWait)
	client.AddRetryCondition(func(r *resty.Response, err error) bool {
		if RPCErrorReason(err) == "local_capacity" {
			return false
		}
		var limited *solanaRateLimitError
		if errors.As(err, &limited) || (r != nil && r.StatusCode() == http.StatusTooManyRequests) {
			return false
		}
		if err != nil {
			return true
		}
		return r != nil && r.StatusCode() >= http.StatusInternalServerError
	})
	client.OnAfterResponse(func(_ *resty.Client, resp *resty.Response) error {
		if resp.StatusCode() == http.StatusTooManyRequests {
			return solRPCThrottle.rateLimited(cooldownKey, resp.Header().Get("Retry-After"))
		}
		return nil
	})

	req := client.R().SetContext(ctx).SetHeader("Content-Type", "application/json")
	if node.ApiKey != "" {
		req.SetHeader("Authorization", "Bearer "+node.ApiKey)
	}
	for key, values := range headers {
		for _, value := range values {
			req.SetHeader(key, value)
		}
	}

	resp, err := req.SetBody(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}).
		Post(rpcUrl)
	if err != nil {
		return nil, &RPCRequestError{Reason: RPCErrorReason(err), cause: err}
	}
	respBody := resp.Body()
	if errValue := gjson.GetBytes(respBody, "error"); errValue.Exists() && errValue.Type != gjson.Null {
		code := errValue.Get("code").String()
		if code == "" {
			code = "unknown"
		}
		return nil, &solanaRPCError{code: code}
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		return nil, rpcStatusError(resp.StatusCode())
	}
	if !gjson.ValidBytes(respBody) || !gjson.GetBytes(respBody, "result").Exists() {
		return nil, &RPCRequestError{Reason: "invalid_response"}
	}
	if method == "getTransaction" && gjson.GetBytes(respBody, "result").Type == gjson.Null {
		return nil, errSolanaTransactionUnavailable
	}
	if method == "getBlockHeight" && !data.ObserveRpcHead(node, gjson.GetBytes(respBody, "result").Int(), RPCRequestStartedAt(resp.RawResponse)) {
		return nil, &RPCRequestError{Reason: "stale_head"}
	}

	return respBody, nil
}

// SolGetSignaturesForAddress returns newest-first signatures newer than untilSig.
// beforeSig paginates backwards within that range; keep untilSig fixed across pages.
func SolGetSignaturesForAddress(address string, limit int, untilSig string, beforeSig string) ([]byte, error) {
	return solGetSignaturesForAddressPage(address, limit, untilSig, beforeSig, nil)
}

// previous contains only signatures accepted from earlier pages of this
// account's walk. Validation is read-only so a rejected endpoint can be retried
// against another endpoint without contaminating pagination state.
func solGetSignaturesForAddressPage(address string, limit int, untilSig string, beforeSig string, previous map[string]bool) ([]byte, error) {
	return solGetSignaturesForAddressPageWithContext(context.Background(), address, limit, untilSig, beforeSig, previous)
}

func solGetSignaturesForAddressPageWithContext(ctx context.Context, address string, limit int, untilSig string, beforeSig string, previous map[string]bool) ([]byte, error) {
	opts := map[string]interface{}{
		"commitment": "finalized",
		"limit":      limit,
	}
	if untilSig != "" {
		opts["until"] = untilSig
	}
	if beforeSig != "" {
		opts["before"] = beforeSig
	}

	return solRetryClientValidatedWithContext(ctx, "getSignaturesForAddress", []interface{}{address, opts}, func(body []byte) error {
		batch, err := parseSolSignatureList(body, limit)
		if err != nil {
			return err
		}
		seen := make(map[string]bool, len(batch))
		for _, sig := range batch {
			if sig.Signature == beforeSig || previous[sig.Signature] || seen[sig.Signature] {
				return fmt.Errorf("signature pagination repeated an entry")
			}
			seen[sig.Signature] = true
		}
		return nil
	})
}

func SolGetBlockHeight() (height int64, err error) {
	defer func() {
		if err != nil {
			data.RecordRpcFailure(mdb.NetworkSolana)
			return
		}
		data.RecordRpcSuccess(mdb.NetworkSolana)
	}()

	node, err := resolveSolanaRpcNode()
	if err != nil {
		return 0, err
	}
	bodyData, err := solRequestWithNode(*node, "getBlockHeight", []interface{}{map[string]interface{}{"commitment": "finalized"}}, nil, "", true)
	if err != nil {
		return 0, err
	}
	height = gjson.GetBytes(bodyData, "result").Int()
	if height <= 0 {
		return 0, fmt.Errorf("unexpected getBlockHeight response")
	}
	return height, nil
}

func SolGetTransaction(sig string) ([]byte, error) {
	txData, err := SolRetryClient("getTransaction", []interface{}{
		sig,
		map[string]interface{}{
			"encoding":                       "jsonParsed",
			"commitment":                     "finalized",
			"maxSupportedTransactionVersion": 0, // suport
		},
	})
	if err != nil {
		var deferred *solanaHistoryBackoffError
		if !errors.As(err, &deferred) || !deferred.cached {
			log.Sugar.Errorf("SolRetryClient failed: %v", err)
		}
		return nil, err
	}

	errField := gjson.GetBytes(txData, "result.meta.err")
	if errField.Exists() && errField.Type != gjson.Null {
		log.Sugar.Warnf("Transaction failed: %v", errField.String())
		return nil, fmt.Errorf("transaction failed: %s", errField.String())
	}

	return txData, nil
}

func solGetTransactionWithURL(rpcURL string, sig string) ([]byte, error) {
	return solGetTransactionWithURLHeaders(rpcURL, sig, nil)
}

func solGetTransactionWithURLHeaders(rpcURL string, sig string, headers http.Header) ([]byte, error) {
	return solGetTransactionWithNodeHeaders(mdb.RpcNode{Network: mdb.NetworkSolana, Type: mdb.RpcNodeTypeHttp, Url: rpcURL}, sig, headers)
}

func solGetTransactionWithNodeHeaders(node mdb.RpcNode, sig string, headers http.Header) ([]byte, error) {
	txData, err := solRetryClientWithNodeHeaders(node, "getTransaction", []interface{}{
		sig,
		map[string]interface{}{
			"encoding":                       "jsonParsed",
			"commitment":                     "finalized",
			"maxSupportedTransactionVersion": 0, // suport
		},
	}, headers)
	if err != nil {
		log.Sugar.Errorf("SolRetryClient failed: %v", err)
		return nil, err
	}

	errField := gjson.GetBytes(txData, "result.meta.err")
	if errField.Exists() && errField.Type != gjson.Null {
		log.Sugar.Warnf("Transaction failed: %v", errField.String())
		return nil, fmt.Errorf("transaction failed: %s", errField.String())
	}

	return txData, nil
}

// isTransferToAddress 判断转账目标是否为指定钱包地址
func isTransferToAddress(transfer *TransferInfo, targetAddress string) bool {
	// Native SOL transfer - check destination directly
	if transfer.Mint == "SOL" {
		return strings.EqualFold(transfer.Destination, targetAddress)
	}

	// Skip Transfer instruction without mint info (use TransferChecked instead)
	if transfer.Mint == "" {
		return false
	}

	// SPL Token transfer - check if destination ATA matches
	return MatchAtaAddress(targetAddress, transfer.Mint, transfer.Destination)
}

// resolveSolTokenAndAmount identifies the token from a transfer using
// the admin-configured chain_tokens list. mintTokens maps SPL mint
// addresses to their ChainToken config; nativeSolToken is non-nil when a
// native SOL entry exists in chain_tokens. Returns ("", 0) for
// transfers whose mint is not configured or below min_amount.
func resolveSolTokenAndAmount(transfer *TransferInfo, mintTokens map[string]*mdb.ChainToken, nativeSolToken *mdb.ChainToken) (string, float64) {
	mint := transfer.Mint

	// Native SOL
	if mint == "SOL" {
		if nativeSolToken == nil {
			return "", 0
		}
		amount := transfer.Amount
		if nativeSolToken.MinAmount > 0 && amount < nativeSolToken.MinAmount {
			return "", 0
		}
		return strings.ToUpper(strings.TrimSpace(nativeSolToken.Symbol)), amount
	}

	cfg, ok := mintTokens[mint]
	if !ok || cfg == nil {
		return "", 0
	}
	decimals := cfg.Decimals
	amount := ADJustAmount(transfer.RawAmount, decimals)
	if cfg.MinAmount > 0 && amount < cfg.MinAmount {
		return "", 0
	}
	return strings.ToUpper(strings.TrimSpace(cfg.Symbol)), amount
}

const (
	// Mint token
	USDT_Mint = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
	USDC_Mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

	USDT_Decimals = 6
	USDC_Decimals = 6
	SOL_Decimals  = 9
)

const (
	// SPL Token instructions
	InstructionTransfer        = 3
	InstructionTransferChecked = 12
)

const (
	// System Program
	SystemProgramID           = "11111111111111111111111111111111"
	InstructionSystemTransfer = 2
)

const (
	TokenProgramID     = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	Token2022ProgramID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
)

// ADJustAmount 将链上原始金额转为可读金额（除以 10^decimals，保留到系统支持的最大匹配精度）
func ADJustAmount(amount uint64, decimals int) float64 {
	if amount == 0 {
		return 0
	}
	decimalAmount := decimal.NewFromBigInt(new(big.Int).SetUint64(amount), 0)
	// 10^decimals
	decimalDivisor := decimal.New(1, int32(decimals))
	adjustedAmount := decimalAmount.Div(decimalDivisor)
	return math.MustParsePrecFloat64(adjustedAmount.InexactFloat64(), data.MaxAmountPrecision)
}

func MatchUsdtAtaAddress(address string, ataTo string) bool {
	ata, err := FindATAAddress(address, USDT_Mint)
	if err != nil {
		log.Sugar.Errorf("FindATAAddress failed: %v", err)
		return false
	}

	return strings.EqualFold(ata, ataTo)
}

func MatchUsdcAtaAddress(address string, ataTo string) bool {
	ata, err := FindATAAddress(address, USDC_Mint)
	if err != nil {
		log.Sugar.Errorf("FindATAAddress failed: %v", err)
		return false
	}

	return strings.EqualFold(ata, ataTo)
}

func MatchAtaAddress(address string, mint string, ataTo string) bool {
	ata, err := FindATAAddress(address, mint)
	if err != nil {
		log.Sugar.Errorf("FindATAAddress failed: %v", err)
		return false
	}

	return strings.EqualFold(ata, ataTo)
}

func FindATAAddress(owner, mint string) (string, error) {
	ownerPubKey, err := solana.PublicKeyFromBase58(owner)
	if err != nil {
		return "", fmt.Errorf("invalid owner public key: %w", err)
	}

	mintPubKey, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return "", fmt.Errorf("invalid mint public key: %w", err)
	}

	ata, _, err := solana.FindAssociatedTokenAddress(ownerPubKey, mintPubKey)
	if err != nil {
		return "", fmt.Errorf("find associated token address failed: %w", err)
	}

	return ata.String(), nil
}

// ParseTransferInfoFromInstruction 从单条指令中解析转账信息，非转账指令返回 nil
func ParseTransferInfoFromInstruction(instruction gjson.Result, txData []byte) (*TransferInfo, error) {
	programID := instruction.Get("programId").String()
	parsedType := instruction.Get("parsed.type").String()

	if programID == SystemProgramID && parsedType == "transfer" {
		return parseSystemTransfer(instruction, txData)
	}

	if programID == TokenProgramID || programID == Token2022ProgramID {
		switch parsedType {
		case "transfer":
			return parseSplTransfer(instruction, txData)
		case "transferChecked":
			return parseSplTransferChecked(instruction, txData)
		}
	}

	// Skip non-transfer instructions (ComputeBudget, AToken create, etc.)
	return nil, nil
}

func parseSystemTransfer(instruction gjson.Result, txData []byte) (*TransferInfo, error) {
	info := instruction.Get("parsed.info")
	source := info.Get("source").String()
	destination := info.Get("destination").String()
	lamports := info.Get("lamports").Uint()
	blockTime := gjson.GetBytes(txData, "result.blockTime").Int()

	return &TransferInfo{
		Source:      source,
		Destination: destination,
		Mint:        "SOL",
		Amount:      ADJustAmount(lamports, SOL_Decimals),
		RawAmount:   lamports,
		BlockTime:   blockTime,
	}, nil
}

// parseSplTransfer resolves unchecked transfers using validated pre/post balances.
func parseSplTransfer(instruction gjson.Result, txData []byte) (*TransferInfo, error) {
	info := instruction.Get("parsed.info")
	source := info.Get("source").String()
	destination := info.Get("destination").String()
	amountStr := info.Get("amount").String()
	blockTime := gjson.GetBytes(txData, "result.blockTime").Int()

	rawAmount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount: %s", amountStr)
	}

	mint, decimals, err := resolveSplTransferAsset(source, destination, txData)
	if err != nil {
		return nil, err
	}

	d := decimals
	return &TransferInfo{
		Source:      source,
		Destination: destination,
		Mint:        mint,
		Amount:      ADJustAmount(rawAmount.Uint64(), decimals),
		RawAmount:   rawAmount.Uint64(),
		Decimals:    &d,
		BlockTime:   blockTime,
	}, nil
}

// parseSplTransferChecked 解析 SPL Token "transferChecked" 指令
func parseSplTransferChecked(instruction gjson.Result, txData []byte) (*TransferInfo, error) {
	info := instruction.Get("parsed.info")
	source := info.Get("source").String()
	destination := info.Get("destination").String()
	mint := info.Get("mint").String()
	amountStr := info.Get("tokenAmount.amount").String()
	decimals := int(info.Get("tokenAmount.decimals").Int())
	blockTime := gjson.GetBytes(txData, "result.blockTime").Int()

	rawAmount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount: %s", amountStr)
	}

	return &TransferInfo{
		Source:      source,
		Destination: destination,
		Mint:        mint,
		Amount:      ADJustAmount(rawAmount.Uint64(), decimals),
		RawAmount:   rawAmount.Uint64(),
		Decimals:    &decimals,
		BlockTime:   blockTime,
	}, nil
}
