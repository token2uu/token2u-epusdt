package task

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/spf13/viper"
)

const rpcProbeTimeout = 5 * time.Second

// RpcHealthJob periodically probes enabled ordinary and paid-fallback nodes.
// Manual verification nodes are left for on-demand admin checks so paid
// endpoints are not consumed by the scheduler. Paid fallback probes run less often.
type RpcHealthJob struct{}

var gRpcHealthJobLock sync.Mutex

func (r RpcHealthJob) Run() {
	if !gRpcHealthJobLock.TryLock() {
		return
	}
	defer gRpcHealthJobLock.Unlock()

	nodes, err := data.ListRpcNodesForHealth()
	if err != nil {
		log.Sugar.Errorf("[rpc-health] list nodes err=%v", err)
		return
	}
	var wg sync.WaitGroup
	for i := range nodes {
		if !nodes[i].Enabled {
			continue
		}
		interval := viper.GetInt("rpc_health_public_interval_seconds")
		if interval <= 0 {
			interval = 60
		}
		if nodes[i].Purpose == mdb.RpcNodePurposePaidFallback {
			interval = viper.GetInt("rpc_health_paid_interval_seconds")
			if interval <= 0 {
				interval = 900
			}
		}
		if nodes[i].LastCheckedAt.Timestamp() > time.Now().Unix()-int64(interval) || data.RpcEndpointCoolingDown(nodes[i]) {
			continue
		}
		wg.Add(1)
		go func(n mdb.RpcNode) {
			defer wg.Done()
			status, latency, err := ProbeConfiguredRpcNode(n)
			if err != nil {
				log.Sugar.Debugf("[rpc-health] probe deferred node=%d", n.ID)
				return
			}
			if err := data.UpdateRpcNodeHealth(n.ID, status, latency); err != nil {
				log.Sugar.Warnf("[rpc-health] update node %d err=%v", n.ID, err)
			}
		}(nodes[i])
	}
	wg.Wait()
}

func ProbeConfiguredRpcNode(node mdb.RpcNode) (string, int, error) {
	if (node.Type == mdb.RpcNodeTypeHttp || node.Type == mdb.RpcNodeTypeIndexer) &&
		(node.Network == mdb.NetworkSolana || node.Network == mdb.NetworkTron || node.Network == mdb.NetworkAptos) {
		start := time.Now()
		if err := service.ProbeRPCNode(node); err != nil {
			if errors.Is(err, service.ErrRPCProbeDeferred) {
				return "", 0, err
			}
			return mdb.RpcNodeStatusDown, -1, nil
		}
		return mdb.RpcNodeStatusOk, int(time.Since(start).Milliseconds()), nil
	}
	status, latency := ProbeNode(node.Url)
	return status, latency, nil
}

// ProbeNode does a TCP dial to the RPC URL and returns (status, latencyMs).
// Exported so the admin controller can reuse it without duplicating logic.
func ProbeNode(rawURL string) (string, int) {
	addr, err := ParseAddress(rawURL)
	if err != nil {
		return mdb.RpcNodeStatusDown, -1
	}
	dur, err := MeasureTCPDial(addr, rpcProbeTimeout)
	if err != nil {
		return mdb.RpcNodeStatusDown, -1
	}
	return mdb.RpcNodeStatusOk, int(dur.Milliseconds())
}

func ParseAddress(raw string) (string, error) {
	if !strings.Contains(raw, "://") {
		raw = "tcp://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		switch u.Scheme {
		case "https", "wss":
			port = "443"
		default:
			port = "80"
		}
	}

	return host + ":" + port, nil
}

func MeasureTCPDial(addr string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return time.Since(start), nil
}
