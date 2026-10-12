package data

import (
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

type RpcRoutingStats struct {
	PublicRequests         int64            `json:"public_requests"`
	PaidRequests           int64            `json:"paid_requests"`
	FailedRequests         int64            `json:"failed_requests"`
	PaidRequestsThisMinute int64            `json:"paid_requests_this_minute"`
	PaidBudgetExceeded     bool             `json:"paid_budget_exceeded"`
	FallbackReasons        map[string]int64 `json:"fallback_reasons"`
	LastScanSuccessAt      time.Time        `json:"last_scan_success_at"`
	ScanStalledSeconds     int64            `json:"scan_stalled_seconds"`
	window                 time.Time
	scanStarted            time.Time
}

var rpcRoutingCounters = struct {
	sync.Mutex
	networks map[string]*RpcRoutingStats
}{networks: make(map[string]*RpcRoutingStats)}

func routingStatsLocked(network string) *RpcRoutingStats {
	s := rpcRoutingCounters.networks[network]
	if s == nil {
		s = &RpcRoutingStats{FallbackReasons: make(map[string]int64)}
		rpcRoutingCounters.networks[network] = s
	}
	if s.window.IsZero() || time.Since(s.window) >= time.Minute {
		s.window = time.Now()
		s.PaidRequestsThisMinute = 0
	}
	return s
}

// Counts actual HTTP attempts, including retries and probes; cache hits and
// requests rejected locally during cooldown do not consume provider credits.
func RecordRpcRequest(node mdb.RpcNode, fallbackReason string, status int) bool {
	rpcRoutingCounters.Lock()
	defer rpcRoutingCounters.Unlock()
	s := routingStatsLocked(node.Network)
	if node.Purpose == mdb.RpcNodePurposePaidFallback {
		s.PaidRequests++
		s.PaidRequestsThisMinute++
	} else {
		s.PublicRequests++
	}
	if status < 200 || status >= 400 {
		s.FailedRequests++
	}
	if fallbackReason != "" {
		s.FallbackReasons[fallbackReason]++
	}
	return node.Purpose == mdb.RpcNodePurposePaidFallback && s.PaidRequestsThisMinute == paidRpcBudget()
}

func paidRpcBudget() int64 {
	n := viper.GetInt64("rpc_paid_requests_per_minute")
	if n <= 0 {
		n = 120
	}
	return n
}

// A soft budget: scanners may defer cold addresses, but payment verification
// and catch-up requests must remain able to reach a paid fallback.
func PaidRpcBudgetExceeded(network string) bool {
	rpcRoutingCounters.Lock()
	defer rpcRoutingCounters.Unlock()
	return routingStatsLocked(network).PaidRequestsThisMinute >= paidRpcBudget()
}

func RecordRpcScanResult(network string, err error) {
	rpcRoutingCounters.Lock()
	defer rpcRoutingCounters.Unlock()
	s := routingStatsLocked(network)
	if s.scanStarted.IsZero() {
		s.scanStarted = time.Now()
	}
	if err == nil {
		s.LastScanSuccessAt = time.Now()
	}
}

func SnapshotRpcRoutingStats() map[string]RpcRoutingStats {
	rpcRoutingCounters.Lock()
	defer rpcRoutingCounters.Unlock()
	out := make(map[string]RpcRoutingStats, len(rpcRoutingCounters.networks))
	for network := range rpcRoutingCounters.networks {
		s := routingStatsLocked(network)
		copy := *s
		copy.PaidBudgetExceeded = s.PaidRequestsThisMinute >= paidRpcBudget()
		since := s.LastScanSuccessAt
		if since.IsZero() {
			since = s.scanStarted
		}
		if !since.IsZero() {
			copy.ScanStalledSeconds = int64(time.Since(since).Seconds())
		}
		copy.FallbackReasons = make(map[string]int64, len(s.FallbackReasons))
		for k, v := range s.FallbackReasons {
			copy.FallbackReasons[k] = v
		}
		out[network] = copy
	}
	return out
}

func ResetRpcRoutingStatsForTest() {
	rpcRoutingCounters.Lock()
	defer rpcRoutingCounters.Unlock()
	rpcRoutingCounters.networks = make(map[string]*RpcRoutingStats)
}
