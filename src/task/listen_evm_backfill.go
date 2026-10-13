package task

import (
	"context"
	"fmt"
	"math/big"
	"strings"
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
	evmBackfillInitialLookbackBlocks = 2048
	evmBackfillBatchBlocks           = 1000
	evmBackfillRetryDelay            = 3 * time.Second
	evmBackfillCatchupDelay          = 250 * time.Millisecond
	evmBackfillHeaderTimeout         = 10 * time.Second
	evmBackfillQueryTimeout          = 30 * time.Second
)

type evmRecipientCheckerFunc func(common.Address) bool

func StartEthereumBackfillScannerListener() {
	startEvmBackfillScanner(mdb.NetworkEthereum, "[ETH-BACKFILL]")
}

func StartBscBackfillScannerListener() {
	startEvmBackfillScanner(mdb.NetworkBsc, "[BSC-BACKFILL]")
}

func StartPolygonBackfillScannerListener() {
	startEvmBackfillScanner(mdb.NetworkPolygon, "[POLYGON-BACKFILL]")
}

func StartPlasmaBackfillScannerListener() {
	startEvmBackfillScanner(mdb.NetworkPlasma, "[PLASMA-BACKFILL]")
}

func startEvmBackfillScanner(network, logPrefix string) {
	for {
		if data.IsChainEnabled(network) {
			if contracts := loadChainTokenContracts(network, logPrefix); len(contracts) > 0 {
				runEvmBackfillScanner(network, logPrefix, contracts)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func runEvmBackfillScanner(network, logPrefix string, contracts []common.Address) {
	ctx, cancel := chainEnabledWatchdog(network, logPrefix, chainTokenFingerprint(network))
	defer cancel()

	wallets, err := data.GetAvailableWalletAddressByNetwork(network)
	if err != nil {
		log.Sugar.Errorf("%s failed to get wallet addresses: %v", logPrefix, err)
		return
	}
	if len(evmRecipientTopicsFromWallets(wallets)) == 0 {
		log.Sugar.Warnf("%s no enabled wallet addresses, scanner idle", logPrefix)
		return
	}
	// Freeze before dialing too: an unavailable WS/HTTP node must not move
	// the first scan's time boundary forward on every reconnect attempt.
	if _, initialized, err := loadEvmScanCursor(network); err != nil {
		log.Sugar.Errorf("%s load bootstrap cursor: %v", logPrefix, err)
		return
	} else if !initialized {
		if _, err := data.EnsureNetworkScanStartTime(network, time.Now()); err != nil {
			log.Sugar.Errorf("%s freeze bootstrap boundary: %v", logPrefix, err)
			return
		}
	}

	node, ok := resolveChainHttpNode(network, logPrefix)
	nodeKind := "HTTP"
	if !ok {
		node, ok = resolveChainWsNode(network, logPrefix)
		nodeKind = "WSS"
	}
	if !ok {
		return
	}
	log.Sugar.Infof("%s connecting using %s node %s watching %d contract(s)", logPrefix, nodeKind, data.RpcNodeLogLabel(node), len(contracts))

	query := ethereum.FilterQuery{
		Addresses: contracts,
	}

	failWait := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}

		dialCtx, cancelDial := context.WithTimeout(ctx, evmNodeDialTimeout)
		client, err := ethclient.DialContext(dialCtx, node.Url)
		cancelDial()
		if err != nil {
			log.Sugar.Warnf("%s dial: %v, retry in %s", logPrefix, err, failWait)
			if recordEvmNodeFailure(logPrefix, network, node, "dial") {
				return
			}
			if !sleepOrDone(ctx, failWait) {
				return
			}
			failWait = nextBackoff(failWait, 60*time.Second)
			continue
		}

		err = runEvmBackfillLoop(ctx, client, network, logPrefix, node, query)
		client.Close()

		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Sugar.Warnf("%s backfill loop stopped: %v, retry in %s", logPrefix, err, failWait)
			if recordEvmNodeFailure(logPrefix, network, node, err.Error()) {
				return
			}
			if !sleepOrDone(ctx, failWait) {
				return
			}
			failWait = nextBackoff(failWait, 60*time.Second)
			continue
		}

		failWait = 2 * time.Second
		if !sleepOrDone(ctx, 3*time.Second) {
			return
		}
	}
}

func runEvmBackfillLoop(ctx context.Context, client *ethclient.Client, network, logPrefix string, node mdb.RpcNode, baseQuery ethereum.FilterQuery) error {
	lastBlock, initialized, err := loadEvmScanCursor(network)
	if err != nil {
		return err
	}
	var start time.Time
	if !initialized {
		start, err = data.EnsureNetworkScanStartTime(network, time.Now())
		if err != nil {
			return fmt.Errorf("freeze backfill boundary: %w", err)
		}
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		chain, interval := evmBackfillChainConfig(network)
		if chain == nil || !chain.Enabled {
			return nil
		}

		requestStarted := time.Now()
		latest, err := latestEvmHeader(ctx, client)
		if err != nil {
			return err
		}
		if latest == nil {
			return nil
		}
		if latest.Number == nil || !latest.Number.IsInt64() || latest.Number.Sign() < 0 {
			return fmt.Errorf("latest block number exceeds int64 range")
		}
		// A successful response from a frozen endpoint must not pin this loop
		// forever. Endpoint state survives reconnects; the caller's existing
		// failure threshold/cooldown selects another node without moving cursor.
		if !data.ObserveRpcHead(node, latest.Number.Int64(), requestStarted) {
			return &service.RPCRequestError{Reason: "stale_head"}
		}
		data.RecordRpcSuccess(network)
		data.RecordRpcBlockHeight(network, latest.Number.Int64())
		log.Sugar.Debugf("%s latest block=%s", logPrefix, latest.Number.String())

		confirmedHead := confirmedEvmHead(latest.Number.Int64(), chain.MinConfirmations)
		if !initialized {
			lastBlock, err = initialEvmBackfillCursor(ctx, client, confirmedHead, start)
			if err != nil {
				return fmt.Errorf("locate initial backfill boundary: %w", err)
			}
			if err := data.UpsertEvmScanCursor(network, lastBlock); err != nil {
				return fmt.Errorf("initialize backfill cursor: %w", err)
			}
			initialized = true
			log.Sugar.Infof("%s initialized backfill cursor at block=%d confirmed_head=%d payment_boundary=%s", logPrefix, lastBlock, confirmedHead, start.UTC().Format(time.RFC3339Nano))
		}

		if lastBlock >= confirmedHead {
			if !sleepOrDone(ctx, interval) {
				return nil
			}
			continue
		}

		fromBlock := lastBlock + 1
		toBlock := fromBlock + evmBackfillBatchSize(network) - 1
		if toBlock > confirmedHead {
			toBlock = confirmedHead
		}

		// Reload contracts per batch as well as recipients. Orders can use a
		// newly enabled token before the listener watchdog reconnects.
		tokens, err := data.ListEnabledChainTokensByNetwork(network)
		if err != nil {
			return fmt.Errorf("load backfill token contracts: %w", err)
		}
		contracts := make([]common.Address, 0, len(tokens))
		for _, token := range tokens {
			contract := strings.TrimSpace(token.ContractAddress)
			if contract == "" {
				continue
			}
			if !common.IsHexAddress(contract) {
				return fmt.Errorf("invalid backfill token contract for %s/%s", network, token.Symbol)
			}
			contracts = append(contracts, common.HexToAddress(contract))
		}
		recipientTopics := loadEvmRecipientTopics(network, logPrefix)
		if len(contracts) == 0 || len(recipientTopics) == 0 {
			if !sleepOrDone(ctx, interval) {
				return nil
			}
			continue
		}
		batchQuery := baseQuery
		batchQuery.Addresses = contracts
		batchQuery.FromBlock = big.NewInt(fromBlock)
		batchQuery.ToBlock = big.NewInt(toBlock)
		batchQuery.Topics = evmTransferTopics(recipientTopics)
		rpcCtx, cancel := context.WithTimeout(ctx, evmBackfillQueryTimeout)
		logs, err := client.FilterLogs(rpcCtx, batchQuery)
		cancel()
		if err != nil {
			return fmt.Errorf("filter logs range=%d-%d: %w", fromBlock, toBlock, err)
		}
		for _, entry := range logs {
			if entry.BlockNumber < uint64(fromBlock) || entry.BlockNumber > uint64(toBlock) {
				return fmt.Errorf("log outside confirmed query range=%d-%d: block=%d", fromBlock, toBlock, entry.BlockNumber)
			}
		}

		// Bind local filtering to the exact recipient snapshot sent to RPC.
		// The WS cache may still predate a newly enabled receiving address.
		recipients := make(map[common.Address]struct{}, len(recipientTopics))
		for _, topic := range recipientTopics {
			recipients[common.BytesToAddress(topic.Bytes())] = struct{}{}
		}
		if err := processEvmBackfillLogs(ctx, client, network, logPrefix, logs, func(address common.Address) bool {
			_, watched := recipients[address]
			return watched
		}); err != nil {
			return err
		}
		if err := data.UpsertEvmScanCursor(network, toBlock); err != nil {
			return fmt.Errorf("save backfill cursor range=%d-%d: %w", fromBlock, toBlock, err)
		}
		lastBlock = toBlock

		data.RecordRpcSuccess(network)
		data.RecordRpcNodeSuccess(node.ID)

		if toBlock < confirmedHead {
			if !sleepOrDone(ctx, evmBackfillCatchupDelay) {
				return nil
			}
			continue
		}
		if !sleepOrDone(ctx, interval) {
			return nil
		}
	}
}

func processEvmBackfillLogs(ctx context.Context, client *ethclient.Client, network, logPrefix string, logs []types.Log, isWatchedRecipient evmRecipientCheckerFunc) error {
	headerCache := make(map[uint64]*types.Header)
	verified := make([]types.Log, 0, len(logs))
	// Validate the complete relevant batch before settlement. A stale response
	// must not partially credit payments or advance the durable scan cursor.
	for _, vLog := range logs {
		if len(vLog.Topics) == 0 {
			return fmt.Errorf("payment log missing event topics")
		}
		if vLog.Topics[0] != transferEventHash {
			continue
		}
		if err := service.ValidateEvmTransferLog(vLog); err != nil {
			return err
		}

		toAddr := common.HexToAddress(vLog.Topics[2].Hex())
		if !isWatchedRecipient(toAddr) {
			continue
		}

		if vLog.Removed || vLog.BlockNumber > uint64(1<<63-1) || vLog.BlockHash == (common.Hash{}) || vLog.TxHash == (common.Hash{}) {
			return fmt.Errorf("invalid or removed payment log network=%s block=%d", network, vLog.BlockNumber)
		}
		header, ok := headerCache[vLog.BlockNumber]
		if !ok {
			var err error
			header, err = latestEvmBlockHeader(ctx, client, vLog.BlockNumber)
			if err != nil {
				return fmt.Errorf("fetch block header network=%s block=%d: %w", network, vLog.BlockNumber, err)
			}
			if header == nil || header.Number == nil || header.Number.Cmp(new(big.Int).SetUint64(vLog.BlockNumber)) != 0 || header.Time == 0 || header.Time > uint64((1<<63-1)/1000) {
				return fmt.Errorf("invalid block header number/timestamp network=%s block=%d", network, vLog.BlockNumber)
			}
			headerCache[vLog.BlockNumber] = header
		}
		if header.Hash() != vLog.BlockHash {
			return fmt.Errorf("payment log does not match canonical block network=%s block=%d", network, vLog.BlockNumber)
		}
		verified = append(verified, vLog)
	}

	for _, vLog := range verified {
		toAddr := common.BytesToAddress(vLog.Topics[2].Bytes())
		blockTsMs := int64(headerCache[vLog.BlockNumber].Time) * 1000
		if err := service.ProcessConfirmedEvmERC20Transfer(network, vLog.Address, toAddr, new(big.Int).SetBytes(vLog.Data), vLog.TxHash.Hex(), blockTsMs, vLog.Index); err != nil {
			return err
		}
	}
	return nil
}

// Keep the existing 2048-block lookback and extend it when the frozen payment
// boundary is older. Never publish an initial cursor after a failed history read.
func initialEvmBackfillCursor(ctx context.Context, client *ethclient.Client, confirmedHead int64, start time.Time) (int64, error) {
	floor := confirmedHead - evmBackfillInitialLookbackBlocks
	if floor <= 0 {
		return 0, nil
	}
	start = start.Truncate(time.Second) // Include the entire boundary second.
	readTime := func(number int64) (int64, error) {
		header, err := latestEvmBlockHeader(ctx, client, uint64(number))
		if err != nil {
			return 0, err
		}
		if header == nil || header.Number == nil || header.Number.Cmp(big.NewInt(number)) != 0 || header.Time == 0 || header.Time > uint64((1<<63-1)/1000) {
			return 0, fmt.Errorf("invalid bootstrap header at %d", number)
		}
		return int64(header.Time) * 1000, nil
	}
	stamp, err := readTime(floor)
	if err != nil {
		return 0, err
	}
	if stamp < start.UnixMilli() {
		return floor, nil
	}
	return paymentScanPredecessor(floor, start, func(number int64) (int64, error) {
		if number == floor {
			return stamp, nil
		}
		return readTime(number)
	})
}

func loadEvmScanCursor(network string) (int64, bool, error) {
	row, err := data.GetEvmScanCursor(network)
	if err != nil {
		return 0, false, err
	}
	if row == nil || row.ID == 0 {
		return 0, false, nil
	}
	if row.LastBlock < 0 {
		return 0, false, fmt.Errorf("invalid negative EVM scan cursor")
	}
	return row.LastBlock, true, nil
}

func evmBackfillChainConfig(network string) (*mdb.Chain, time.Duration) {
	chain, err := data.GetChainByNetwork(network)
	if err != nil || chain == nil {
		return nil, 5 * time.Second
	}
	interval := time.Duration(chain.ScanIntervalSec) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return chain, interval
}

func evmBackfillBatchSize(network string) int64 {
	if network == mdb.NetworkBsc {
		return 200
	}
	return evmBackfillBatchBlocks
}

func confirmedEvmHead(head int64, minConfirmations int) int64 {
	if minConfirmations <= 1 {
		return head
	}
	conf := int64(minConfirmations)
	if head < conf-1 {
		return 0
	}
	return head - conf + 1
}

func latestEvmHeader(ctx context.Context, client *ethclient.Client) (*types.Header, error) {
	headerCtx, cancel := context.WithTimeout(ctx, evmBackfillHeaderTimeout)
	defer cancel()

	header, err := client.HeaderByNumber(headerCtx, nil)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("latest header: %w", err)
	}
	return header, nil
}

func latestEvmBlockHeader(ctx context.Context, client *ethclient.Client, blockNumber uint64) (*types.Header, error) {
	headerCtx, cancel := context.WithTimeout(ctx, evmBackfillHeaderTimeout)
	defer cancel()
	return client.HeaderByNumber(headerCtx, big.NewInt(int64(blockNumber)))
}
