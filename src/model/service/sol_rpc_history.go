package service

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Preserve the RPC code without logging provider messages, which may contain
// credentials. Missing history is specific to a query, not the whole node.
type solanaRPCError struct{ code string }

func (e *solanaRPCError) Error() string { return "solana rpc error code=" + e.code }

var errSolanaTransactionUnavailable = errors.New("solana transaction unavailable at requested commitment")

func isSolanaHistoryUnavailable(err error) bool {
	var rpcErr *solanaRPCError
	return errors.Is(err, errSolanaTransactionUnavailable) || (errors.As(err, &rpcErr) && rpcErr.code == "-32020")
}

type solanaHistoryBackoffError struct {
	until  time.Time
	cached bool
	cause  error
}

func (e *solanaHistoryBackoffError) Error() string {
	return fmt.Sprintf("%v (history query cooldown until %s)", e.cause, e.until.UTC().Format(time.RFC3339))
}

func (e *solanaHistoryBackoffError) Unwrap() error { return e.cause }

type solanaHistoryFailure struct {
	failures int
	until    time.Time
	updated  time.Time
	cause    error
}

type solanaHistoryBackoff struct {
	mu      sync.Mutex
	entries map[[32]byte]solanaHistoryFailure
	group   singleflight.Group
	now     func() time.Time
	nextGC  time.Time
}

func newSolanaHistoryBackoff() *solanaHistoryBackoff {
	return &solanaHistoryBackoff{entries: make(map[[32]byte]solanaHistoryFailure), now: time.Now}
}

var solHistoryBackoff = newSolanaHistoryBackoff()

// Only history reads use this gate. Health/statistics reads and unrelated
// wallets cannot clear a failing query's cooldown. Endpoint and auth headers
// are part of the identity so a different provider/credential can still work.
func (g *solanaHistoryBackoff) do(endpoint, method string, params []interface{}, headers http.Header, request func() ([]byte, error)) ([]byte, error) {
	if method != "getSignaturesForAddress" && method != "getTransaction" {
		return request()
	}
	identity, err := json.Marshal([]interface{}{endpoint, method, params, headers})
	if err != nil {
		return nil, err
	}
	key := sha256.Sum256(identity)
	result, err, _ := g.group.Do(string(key[:]), func() (interface{}, error) {
		g.mu.Lock()
		now := g.now()
		if !now.Before(g.nextGC) {
			for k, entry := range g.entries {
				if now.Sub(entry.updated) >= 24*time.Hour {
					delete(g.entries, k)
				}
			}
			g.nextGC = now.Add(time.Hour)
		}
		entry := g.entries[key]
		if now.Before(entry.until) {
			g.mu.Unlock()
			return nil, &solanaHistoryBackoffError{until: entry.until, cached: true, cause: entry.cause}
		}
		g.mu.Unlock()

		body, requestErr := request()
		g.mu.Lock()
		defer g.mu.Unlock()
		if isSolanaHistoryUnavailable(requestErr) {
			// Retain the failure count after a cooldown expires; otherwise an
			// unavailable historical cursor would be retried every 30 seconds.
			delays := [...]time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}
			if entry.failures < len(delays) {
				entry.failures++
			}
			entry.updated = g.now()
			entry.until = entry.updated.Add(delays[entry.failures-1])
			entry.cause = requestErr
			g.entries[key] = entry
			return nil, &solanaHistoryBackoffError{until: entry.until, cause: requestErr}
		}
		if requestErr == nil {
			delete(g.entries, key)
		}
		return body, requestErr
	})
	if err != nil {
		return nil, err
	}
	return result.([]byte), nil
}
