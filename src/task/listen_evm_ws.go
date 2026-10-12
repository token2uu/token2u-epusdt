package task

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	evmStatsIdleInterval  = 30 * time.Second
	evmStatsHeaderTimeout = 10 * time.Second
)

// A failed header read must leave the event to confirmed backfill. Local time
// cannot prove historical ownership and must never enter a durable receipt.
func processEvmWsTransfer(ctx context.Context, client *ethclient.Client, network, logPrefix string, event types.Log, isWatched evmRecipientCheckerFunc) {
	if event.Removed || len(event.Topics) < 3 || event.Topics[0] != transferEventHash {
		return
	}
	recipient := common.HexToAddress(event.Topics[2].Hex())
	if !isWatched(recipient) {
		return
	}
	header, err := latestEvmBlockHeader(ctx, client, event.BlockNumber)
	if err == nil && (header == nil || header.Number == nil || header.Number.Cmp(new(big.Int).SetUint64(event.BlockNumber)) != 0 || header.Time == 0 || header.Time > uint64((1<<63-1)/1000)) {
		err = fmt.Errorf("missing or invalid block header timestamp/number")
	}
	if err != nil {
		if ctx.Err() == nil {
			data.RecordRpcFailure(network)
			log.Sugar.Warnf("%s header unavailable block=%d tx=%s: %v; deferring payment to confirmed backfill", logPrefix, event.BlockNumber, event.TxHash.Hex(), err)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}
	data.RecordRpcSuccess(network)
	service.TryProcessEvmERC20Transfer(network, event.Address, recipient, new(big.Int).SetBytes(event.Data), event.TxHash.Hex(), int64(header.Time)*1000, event.Index)
}

// runEvmWsLogListener connects to node.Url, subscribes to Transfer logs,
// and dispatches each log to handleLog. It retries on transient errors
// with exponential backoff until the node reaches the failure threshold.
// The ctx lets the caller trigger a clean exit — e.g. when admin disables
// the chain, the caller cancels the context and the function returns.
func runEvmWsLogListener(ctx context.Context, network string, logPrefix string, node mdb.RpcNode, query ethereum.FilterQuery, handleLog func(*ethclient.Client, types.Log)) {
	const (
		minBackoff       = 2 * time.Second
		maxBackoff       = 60 * time.Second
		rejoinWait       = 3 * time.Second
		stableResetAfter = 60 * time.Second
	)
	failWait := minBackoff
	wsURL := node.Url
	nodeLabel := data.RpcNodeLogLabel(node)

	for {
		if ctx.Err() != nil {
			return
		}

		dialCtx, cancelDial := context.WithTimeout(ctx, evmNodeDialTimeout)
		client, err := ethclient.DialContext(dialCtx, wsURL)
		cancelDial()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Sugar.Warnf("%s dial: %v, retry in %s", logPrefix, err, failWait)
			if recordEvmNodeFailure(logPrefix, network, node, "dial") {
				return
			}
			if !sleepOrDone(ctx, failWait) {
				return
			}
			failWait = nextBackoff(failWait, maxBackoff)
			continue
		}

		logsCh := make(chan types.Log)
		sub, err := client.SubscribeFilterLogs(ctx, query, logsCh)
		if err != nil {
			client.Close()
			if ctx.Err() != nil {
				return
			}
			log.Sugar.Warnf("%s subscribe: %v, retry in %s", logPrefix, err, failWait)
			if recordEvmNodeFailure(logPrefix, network, node, "subscribe") {
				return
			}
			if !sleepOrDone(ctx, failWait) {
				return
			}
			failWait = nextBackoff(failWait, maxBackoff)
			continue
		}
		failWait = minBackoff
		data.RecordRpcSuccess(network)

		log.Sugar.Infof("%s connected, subscribed to Transfer logs using WSS node %s", logPrefix, nodeLabel)

		connectedAt := time.Now()
		recvErr := recvLoop(ctx, client, sub, logsCh, network, logPrefix, handleLog)

		if ctx.Err() != nil {
			return
		}
		if time.Since(connectedAt) >= stableResetAfter {
			if recvErr != nil {
				data.RecordRpcFailure(network)
			}
			data.RecordRpcNodeSuccess(node.ID)
		} else if recvErr != nil && recordEvmNodeFailure(logPrefix, network, node, recvErr.Error()) {
			return
		}
		if !sleepOrDone(ctx, rejoinWait) {
			return
		}
	}
}

func recvLoop(ctx context.Context, client *ethclient.Client, sub ethereum.Subscription, logsCh <-chan types.Log, network string, logPrefix string, handleLog func(*ethclient.Client, types.Log)) error {
	statsCtx, cancelStats := context.WithCancel(ctx)
	statsTicker := time.NewTicker(evmStatsIdleInterval)
	headerStatsDone := make(chan bool, 1)
	lastStatsUpdateAt := time.Now()
	headerStatsInFlight := false
	requestLatestHeader := func() {
		if headerStatsInFlight {
			return
		}
		headerStatsInFlight = true
		go func() {
			headerStatsDone <- recordEvmLatestHeader(statsCtx, network, logPrefix, client)
		}()
	}

	defer func() {
		cancelStats()
		statsTicker.Stop()
		sub.Unsubscribe()
		client.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Sugar.Infof("%s context cancelled, stopping", logPrefix)
			return nil
		case <-statsTicker.C:
			if shouldRefreshEvmLatestHeader(lastStatsUpdateAt, headerStatsInFlight, time.Now()) {
				requestLatestHeader()
			}
		case updated := <-headerStatsDone:
			headerStatsInFlight = false
			if updated {
				lastStatsUpdateAt = time.Now()
			}
		case err := <-sub.Err():
			if err != nil {
				log.Sugar.Warnf("%s subscription error: %v, reconnecting", logPrefix, err)
				return fmt.Errorf("subscription error")
			} else {
				log.Sugar.Warnf("%s subscription closed, reconnecting", logPrefix)
				return fmt.Errorf("subscription closed")
			}
		case vLog, ok := <-logsCh:
			if !ok {
				log.Sugar.Warnf("%s log channel closed, reconnecting", logPrefix)
				return fmt.Errorf("log channel closed")
			}
			if recordEvmLogBlockHeight(network, logPrefix, vLog.BlockNumber) {
				lastStatsUpdateAt = time.Now()
			}
			handleLog(client, vLog)
		}
	}
}

func shouldRefreshEvmLatestHeader(lastStatsUpdateAt time.Time, inFlight bool, now time.Time) bool {
	return !inFlight && now.Sub(lastStatsUpdateAt) >= evmStatsIdleInterval
}

func recordEvmLogBlockHeight(network string, logPrefix string, blockNumber uint64) bool {
	if blockNumber == 0 {
		return false
	}
	if blockNumber > uint64(1<<63-1) {
		log.Sugar.Warnf("%s log block number exceeds int64 range: %d", logPrefix, blockNumber)
		return false
	}
	data.RecordRpcBlockHeight(network, int64(blockNumber))
	return true
}

func recordEvmLatestHeader(ctx context.Context, network string, logPrefix string, client *ethclient.Client) bool {
	headerCtx, cancel := context.WithTimeout(ctx, evmStatsHeaderTimeout)
	defer cancel()

	header, err := client.HeaderByNumber(headerCtx, nil)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		data.RecordRpcFailure(network)
		log.Sugar.Warnf("%s latest header stats failed: %v", logPrefix, err)
		return false
	}
	data.RecordRpcSuccess(network)
	return recordEvmHeaderBlockHeight(network, logPrefix, header)
}

func recordEvmHeaderBlockHeight(network string, logPrefix string, header *types.Header) bool {
	if header == nil || header.Number == nil {
		return false
	}
	if !header.Number.IsInt64() {
		log.Sugar.Warnf("%s latest header block number exceeds int64 range: %s", logPrefix, header.Number.String())
		return false
	}
	data.RecordRpcBlockHeight(network, header.Number.Int64())
	return true
}

func recordEvmNodeFailure(logPrefix string, network string, node mdb.RpcNode, reason string) bool {
	data.RecordRpcFailure(network)
	failures, cooling := data.RecordRpcNodeFailure(node.ID)
	nodeLabel := data.RpcNodeLogLabel(node)
	if !cooling {
		log.Sugar.Warnf("%s node failed (%s), node=%s failures=%d/%d", logPrefix, reason, nodeLabel, failures, data.RpcFailoverThreshold)
		return false
	}
	log.Sugar.Warnf("%s node reached fail threshold (%s), node=%s, resolving another node", logPrefix, reason, nodeLabel)
	return true
}

// sleepOrDone waits for d or for ctx cancellation, whichever comes
// first. Returns true if the sleep completed normally, false if ctx
// was cancelled (caller should exit).
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	n := cur * 2
	if n > max {
		return max
	}
	return n
}
