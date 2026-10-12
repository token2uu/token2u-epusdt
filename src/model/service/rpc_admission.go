package service

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"golang.org/x/time/rate"
)

// All budgets are checked and consumed together immediately before dispatch.
// Waiting requests hold no tokens, so a slow endpoint cannot compress another
// budget's spacing or reserve capacity that a cancelled request never uses.
var rpcAdmissionMu sync.Mutex

func waitRPCAdmission(ctx context.Context, node mdb.RpcNode) error {
	for {
		if err := ctx.Err(); err != nil {
			return &RPCRequestError{Reason: "local_capacity", cause: err}
		}
		delay, err := tryRPCAdmission(ctx, node)
		if err != nil {
			return &RPCRequestError{Reason: "local_capacity", cause: err}
		}
		if delay <= 0 {
			return nil
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
			return &RPCRequestError{Reason: "local_capacity", cause: context.DeadlineExceeded}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &RPCRequestError{Reason: "local_capacity", cause: ctx.Err()}
		case <-timer.C:
		}
	}
}

func tryRPCAdmission(ctx context.Context, node mdb.RpcNode) (time.Duration, error) {
	rpcAdmissionMu.Lock()
	defer rpcAdmissionMu.Unlock()
	now := time.Now()
	if node.Network == mdb.NetworkSolana {
		solRPCThrottle.mu.Lock()
		defer solRPCThrottle.mu.Unlock()
		key := solThrottleIdentity(node.Url, node.ApiKey)
		if until := solRPCThrottle.cooldowns[key]; now.Before(until) {
			return 0, &solanaRateLimitError{until: until}
		}
		delete(solRPCThrottle.cooldowns, key)
	}
	if data.RpcEndpointCoolingDown(node) {
		return 0, &RPCRequestError{Reason: "cooldown"}
	}
	endpoint := endpointLimiter(node)
	delay := rpcBudgetDelay(endpoint, now)
	var shared *rate.Limiter
	if node.Network == mdb.NetworkSolana {
		delay = max(delay, solRPCThrottle.next.Sub(now))
	} else if node.Network == mdb.NetworkAptos && node.Type == mdb.RpcNodeTypeIndexer {
		shared = aptosIndexerLimiter
		delay = max(delay, rpcBudgetDelay(shared, now))
	}
	if delay > 0 {
		return delay, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// Every production consumer of these limiters holds rpcAdmissionMu. The
	// readiness check and AllowN use the same time and cannot race a consumer.
	endpoint.AllowN(now, 1)
	if shared != nil {
		shared.AllowN(now, 1)
	}
	if node.Network == mdb.NetworkSolana {
		solRPCThrottle.next = now.Add(config.GetSolanaRPCInterval())
	}
	return 0, nil
}

func rpcBudgetDelay(limiter *rate.Limiter, now time.Time) time.Duration {
	limit := limiter.Limit()
	if limit == rate.Inf {
		return 0
	}
	if limit <= 0 || limiter.Burst() < 1 {
		return time.Duration(math.MaxInt64)
	}
	nanos := math.Ceil((1 - limiter.TokensAt(now)) * float64(time.Second) / float64(limit))
	if nanos <= 0 {
		return 0
	}
	if nanos >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(nanos)
}
