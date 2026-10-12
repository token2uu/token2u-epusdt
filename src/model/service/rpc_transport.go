package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
)

type RPCRequestError struct {
	Reason string
	Status int
	cause  error
}

func (e *RPCRequestError) Unwrap() error { return e.cause }

func (e *RPCRequestError) Error() string {
	return fmt.Sprintf("RPC request failed: %s (HTTP %d)", e.Reason, e.Status)
}

func RPCErrorReason(err error) string {
	if err == nil {
		return ""
	}
	var request *RPCRequestError
	if errors.As(err, &request) {
		return request.Reason
	}
	var limited *solanaRateLimitError
	if errors.As(err, &limited) {
		return "rate_limited"
	}
	if isSolanaHistoryUnavailable(err) {
		return "history_unavailable"
	}
	return "request_failed"
}

func WrapRPCError(err error) error {
	if err == nil {
		return nil
	}
	return &RPCRequestError{Reason: RPCErrorReason(err), cause: err}
}

type rpcLimiterEntry struct {
	limiter *rate.Limiter
	rps     float64
}

var rpcLimiters = struct {
	sync.Mutex
	entries map[string]rpcLimiterEntry
}{entries: make(map[string]rpcLimiterEntry)}

func endpointLimiter(node mdb.RpcNode) *rate.Limiter {
	key, rps := node.Url+"\x00"+node.ApiKey, viper.GetFloat64("rpc_public_requests_per_second")
	if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
		rps = 2
	}
	if node.Purpose == mdb.RpcNodePurposePaidFallback {
		rps = viper.GetFloat64("rpc_paid_requests_per_second")
		if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
			rps = 5
		}
	}
	rpcLimiters.Lock()
	defer rpcLimiters.Unlock()
	entry, ok := rpcLimiters.entries[key]
	if !ok {
		entry = rpcLimiterEntry{limiter: rate.NewLimiter(rate.Limit(rps), 1), rps: rps}
	} else if entry.rps != rps {
		entry.limiter.SetLimit(rate.Limit(rps))
		entry.rps = rps
	}
	rpcLimiters.entries[key] = entry
	return entry.limiter
}

type rpcTransport struct {
	node   mdb.RpcNode
	reason string
	base   http.RoundTripper
}

type rpcRequestStartKey struct{}

// RPCRequestStartedAt identifies the actual HTTP attempt, after local queues.
// Each retry/redirect receives its own timestamp, shared by head observers.
func RPCRequestStartedAt(response *http.Response) time.Time {
	if response == nil || response.Request == nil {
		return time.Time{}
	}
	started, _ := response.Request.Context().Value(rpcRequestStartKey{}).(time.Time)
	return started
}

// NewRPCTransport coordinates endpoint/shared limits and cooldowns, accounting
// only real network requests. Callers retain protocol validation/cursor rules.
func NewRPCTransport(node mdb.RpcNode, reason string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &rpcTransport{node: node, reason: reason, base: base}
}

func (t *rpcTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := waitRPCAdmission(req.Context(), t.node); err != nil {
		return nil, err
	}
	req = req.WithContext(context.WithValue(req.Context(), rpcRequestStartKey{}, time.Now()))
	resp, err := t.base.RoundTrip(req)
	status := 0
	if resp != nil {
		resp.Request = req
		status = resp.StatusCode
	}
	if data.RecordRpcRequest(t.node, t.reason, status) {
		log.Sugar.Warnf("[RPC] paid request budget reached network=%s; deferring cold addresses, payment verification remains enabled", t.node.Network)
	}
	if status == http.StatusTooManyRequests {
		data.CooldownRpcEndpoint(t.node, resp.Header.Get("Retry-After"))
	}
	if err != nil {
		return resp, &RPCRequestError{Reason: "network_error", cause: err}
	}
	return resp, nil
}

func RPCRequestTimeout(node mdb.RpcNode, maximum time.Duration) time.Duration {
	seconds := viper.GetInt("rpc_public_timeout_seconds")
	if seconds <= 0 {
		seconds = 5
	}
	if node.Purpose == mdb.RpcNodePurposePaidFallback {
		seconds = viper.GetInt("rpc_paid_timeout_seconds")
		if seconds <= 0 {
			seconds = 10
		}
	}
	if maximum <= 0 {
		return time.Duration(seconds) * time.Second
	}
	return min(maximum, time.Duration(seconds)*time.Second)
}

func rpcHTTPClient(node mdb.RpcNode, reason string, timeout time.Duration) *http.Client {
	return &http.Client{Timeout: RPCRequestTimeout(node, timeout), Transport: NewRPCTransport(node, reason, nil)}
}

func rpcStatusError(status int) error {
	reason := "http_error"
	if status == http.StatusTooManyRequests {
		reason = "rate_limited"
	} else if status >= 500 {
		reason = "server_error"
	} else if status == http.StatusNotFound {
		reason = "history_unavailable"
	}
	return &RPCRequestError{Reason: reason, Status: status}
}

// RPCStatusError avoids logging response bodies/URLs that can include keys.
func RPCStatusError(status int) error { return rpcStatusError(status) }
