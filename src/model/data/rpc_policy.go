package data

import (
	"crypto/sha256"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

type rpcEndpointState struct {
	until    time.Time
	head     int64
	advanced time.Time
}

var rpcEndpointPolicy = struct {
	sync.Mutex
	entries map[[32]byte]rpcEndpointState
	now     func() time.Time
}{entries: make(map[[32]byte]rpcEndpointState), now: time.Now}

func rpcEndpointKey(node mdb.RpcNode) [32]byte {
	return sha256.Sum256([]byte(node.Network + "\x00" + node.Type + "\x00" + node.Url + "\x00" + node.ApiKey))
}

// Keep endpoint-wide throttling independent from success/failure counters:
// a successful in-flight health read must not erase another request's 429.
func CooldownRpcEndpoint(node mdb.RpcNode, retryAfter string) {
	rpcEndpointPolicy.Lock()
	defer rpcEndpointPolicy.Unlock()
	now := rpcEndpointPolicy.now()
	until := now.Add(30 * time.Second)
	if seconds, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && seconds > 0 && seconds <= int64((1<<63-1)/time.Second) {
		until = now.Add(time.Duration(seconds) * time.Second)
	} else if date, err := http.ParseTime(retryAfter); err == nil && date.After(now) {
		until = date
	}
	key := rpcEndpointKey(node)
	state := rpcEndpointPolicy.entries[key]
	if until.After(state.until) {
		state.until = until
	}
	rpcEndpointPolicy.entries[key] = state
}

func RpcEndpointCoolingDown(node mdb.RpcNode) bool {
	rpcEndpointPolicy.Lock()
	cooling := rpcEndpointPolicy.now().Before(rpcEndpointPolicy.entries[rpcEndpointKey(node)].until)
	rpcEndpointPolicy.Unlock()
	return cooling || IsRpcNodeCoolingDown(node.ID)
}

// Detect a provider that responds successfully but no longer advances. This is
// only fed real chain/indexer heads, never an idle wallet's signature cursor.
// requestStarted is the HTTP dispatch time after local rate-limit admission.
func ObserveRpcHead(node mdb.RpcNode, head int64, requestStarted time.Time) bool {
	rpcEndpointPolicy.Lock()
	defer rpcEndpointPolicy.Unlock()
	key, now := rpcEndpointKey(node), rpcEndpointPolicy.now()
	state := rpcEndpointPolicy.entries[key]
	if state.advanced.IsZero() || head > state.head {
		state.head, state.advanced = head, now
		rpcEndpointPolicy.entries[key] = state
		return true
	}
	seconds := viper.GetInt("rpc_stale_head_seconds")
	if seconds <= 0 {
		seconds = 120
	}
	// A request already in flight when the higher head was observed may carry
	// an older snapshot. Accept it without rewinding the high-water mark or
	// refreshing its age; the caller still validates its own required range.
	overlappedAdvance := !requestStarted.IsZero() && requestStarted.Before(state.advanced)
	return (head >= state.head || overlappedAdvance) && now.Sub(state.advanced) < time.Duration(seconds)*time.Second
}

func ResetRpcEndpointPolicyForTest() {
	rpcEndpointPolicy.Lock()
	defer rpcEndpointPolicy.Unlock()
	rpcEndpointPolicy.entries = make(map[[32]byte]rpcEndpointState)
	rpcEndpointPolicy.now = time.Now
}
