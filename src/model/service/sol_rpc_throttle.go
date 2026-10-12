package service

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// All Solana HTTP calls (including retries and probes) share one request budget.
// Endpoint cooldowns are separate so a limited primary does not block a backup.
var solRPCThrottle = &solanaRPCThrottle{cooldowns: make(map[string]time.Time)}

func solThrottleIdentity(endpoint, credential string) string {
	identity := sha256.Sum256([]byte(endpoint + "\x00" + credential))
	return string(identity[:])
}

type solanaRPCThrottle struct {
	mu        sync.Mutex
	next      time.Time
	cooldowns map[string]time.Time
}

type solanaRateLimitError struct{ until time.Time }

func (e *solanaRateLimitError) Error() string {
	return fmt.Sprintf("solana rpc HTTP 429 (cooldown until %s)", e.until.UTC().Format(time.RFC3339))
}

func (g *solanaRPCThrottle) rateLimited(endpoint, retryAfter string) *solanaRateLimitError {
	now := time.Now()
	until := now.Add(30 * time.Second)
	retryAfter = strings.TrimSpace(retryAfter)
	if seconds, err := strconv.ParseInt(retryAfter, 10, 64); err == nil && seconds > 0 && seconds <= int64((1<<63-1)/time.Second) {
		until = now.Add(time.Duration(seconds) * time.Second)
	} else if date, err := http.ParseTime(retryAfter); err == nil && date.After(now) {
		until = date
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// Concurrent responses must never shorten an existing cooldown.
	if previous := g.cooldowns[endpoint]; previous.After(until) {
		until = previous
	}
	g.cooldowns[endpoint] = until
	return &solanaRateLimitError{until: until}
}
