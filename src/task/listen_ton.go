package task

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/jetton"
)

const (
	tonMasterShard           = int64(-0x8000000000000000)
	tonScannerConnectTimeout = 20 * time.Second
	tonScannerRequestTimeout = 15 * time.Second
	tonScannerRetryDelay     = 3 * time.Second
	tonCatchupYield          = 250 * time.Millisecond
	tonMaxMasterBlocksTick   = uint32(50)
	tonBlockTxPageSize       = uint32(256)
)

type tonWatchState struct {
	wallets      map[string]*address.Address
	nativeToken  *mdb.ChainToken
	jettonTokens []mdb.ChainToken
}

func (s *tonWatchState) hasWalletInWorkchain(workchain int32) bool {
	if s == nil {
		return false
	}
	for _, wallet := range s.wallets {
		if wallet != nil && wallet.Workchain() == workchain {
			return true
		}
	}
	return false
}

type tonRuntimeCursor struct {
	persistent      bool
	initialized     bool
	lastMasterSeqNo uint32
	shardSeqNo      map[string]uint32
}

func (c *tonRuntimeCursor) ensureShardSeqNo() {
	if c.shardSeqNo == nil {
		c.shardSeqNo = make(map[string]uint32)
	}
}

type tonJettonWalletCache struct {
	sync.Mutex
	walletByOwnerToken map[string]string
}

func newTonJettonWalletCache() *tonJettonWalletCache {
	return &tonJettonWalletCache{walletByOwnerToken: make(map[string]string)}
}

// StartTonBlockScannerListener runs a forward-only TON masterchain scanner.
func StartTonBlockScannerListener() {
	cache := newTonJettonWalletCache()
	cursor := &tonRuntimeCursor{persistent: true}
	for {
		chain, interval := tonChainConfig()
		if chain == nil || !chain.Enabled {
			sleepOrDone(context.Background(), interval)
			continue
		}
		if !cursor.initialized {
			if err := prepareTonBootstrap(cursor); err != nil {
				log.Sugar.Errorf("[TON] bootstrap boundary: %v", err)
				sleepOrDone(context.Background(), tonScannerRetryDelay)
				continue
			}
		}
		node, ok := resolveTonLiteNode()
		if !ok {
			sleepOrDone(context.Background(), interval)
			continue
		}
		log.Sugar.Infof("[TON] connecting using lite node %s", data.RpcNodeLogLabel(node))
		api, closeFn, err := connectTonLiteAPI(node)
		if err != nil {
			log.Sugar.Warnf("[TON] connect lite node %s failed: %v", data.RpcNodeLogLabel(node), err)
			recordTonLiteNodeFailure(node, "connect")
			sleepOrDone(context.Background(), tonScannerRetryDelay)
			continue
		}
		log.Sugar.Infof("[TON] connected using lite node %s", data.RpcNodeLogLabel(node))
		err = runTonScanner(context.Background(), api, cache, cursor, node)
		closeFn()
		if err != nil {
			log.Sugar.Warnf("[TON] scanner stopped: %v", err)
			recordTonLiteNodeFailure(node, err.Error())
			sleepOrDone(context.Background(), tonScannerRetryDelay)
			continue
		}
	}
}

func runTonScanner(ctx context.Context, api ton.APIClientWrapped, cache *tonJettonWalletCache, cursor *tonRuntimeCursor, node mdb.RpcNode) error {
	if cursor == nil {
		cursor = &tonRuntimeCursor{}
	}
	watchSignature := ""
	for {
		chain, interval := tonChainConfig()
		if chain == nil || !chain.Enabled {
			return nil
		}
		state, err := loadTonWatchState()
		if err != nil {
			return err
		}
		if len(state.wallets) == 0 || (state.nativeToken == nil && len(state.jettonTokens) == 0) {
			log.Sugar.Debug("[TON] no enabled wallets or tokens, idling")
			if !sleepOrDone(ctx, interval) {
				return nil
			}
			continue
		}
		if sig := tonWatchStateSignature(state); sig != watchSignature {
			watchSignature = sig
			log.Sugar.Infof(
				"[TON] watching wallets=%d native_ton=%t jetton_tokens=%d wallet_workchains=%s",
				len(state.wallets),
				state.nativeToken != nil,
				len(state.jettonTokens),
				tonWalletWorkchainSummary(state),
			)
		}

		if !cursor.initialized {
			if err := prepareTonBootstrap(cursor); err != nil {
				return err
			}
		}
		head, err := api.CurrentMasterchainInfo(ctx)
		if err != nil {
			data.RecordRpcFailure(mdb.NetworkTon)
			return fmt.Errorf("current masterchain info: %w", err)
		}
		if head == nil {
			return fmt.Errorf("current masterchain head is nil")
		}
		if !data.ObserveRpcPoolHead(node, int64(head.SeqNo)) {
			return &service.RPCRequestError{Reason: "stale_head"}
		}
		confirmedHeadSeqNo := tonConfirmedMasterSeqNo(head.SeqNo, chain.MinConfirmations)
		data.RecordRpcSuccess(mdb.NetworkTon)
		data.RecordRpcBlockHeight(mdb.NetworkTon, int64(head.SeqNo))

		if !cursor.initialized {
			if err = initializeTonRuntimeCursor(ctx, api, head, confirmedHeadSeqNo, cursor); err != nil {
				data.RecordRpcFailure(mdb.NetworkTon)
				return err
			}
			log.Sugar.Infof(
				"[TON] current masterchain head=%d confirmed=%d root=%s file=%s",
				head.SeqNo,
				confirmedHeadSeqNo,
				shortTonHash(head.RootHash),
				shortTonHash(head.FileHash),
			)
			log.Sugar.Infof("[TON] initialized runtime cursor at masterchain seqno=%d head=%d", cursor.lastMasterSeqNo, head.SeqNo)
			if !sleepOrDone(ctx, interval) {
				return nil
			}
			continue
		}
		if cursor.lastMasterSeqNo >= confirmedHeadSeqNo {
			if !sleepOrDone(ctx, interval) {
				return nil
			}
			continue
		}

		target := cursor.lastMasterSeqNo + tonMaxMasterBlocksTick
		if target > confirmedHeadSeqNo {
			target = confirmedHeadSeqNo
		}
		log.Sugar.Infof("[TON] processing masterchain blocks from=%d to=%d head=%d confirmed=%d", cursor.lastMasterSeqNo+1, target, head.SeqNo, confirmedHeadSeqNo)
		for seq := cursor.lastMasterSeqNo + 1; seq <= target; seq++ {
			master, err := api.WaitForBlock(seq).LookupBlock(ctx, address.MasterchainID, tonMasterShard, seq)
			if err != nil {
				data.RecordRpcFailure(mdb.NetworkTon)
				return fmt.Errorf("lookup masterchain seqno=%d: %w", seq, err)
			}
			if err = processTonMasterBlockWithCursor(ctx, api, master, state, cache, cursor); err != nil {
				data.RecordRpcFailure(mdb.NetworkTon)
				return fmt.Errorf("process masterchain seqno=%d: %w", seq, err)
			}
			if cursor.persistent {
				if err := data.SavePaymentCursor("ton:master", strconv.FormatUint(uint64(master.SeqNo), 10)); err != nil {
					return err
				}
			}
			cursor.lastMasterSeqNo = master.SeqNo
			data.RecordRpcSuccess(mdb.NetworkTon)
			data.RecordRpcBlockHeight(mdb.NetworkTon, int64(seq))
		}
		if target < confirmedHeadSeqNo {
			if !sleepOrDone(ctx, tonCatchupYield) {
				return nil
			}
		}
	}
}

func tonConfirmedMasterSeqNo(headSeqNo uint32, minConfirmations int) uint32 {
	if minConfirmations <= 1 {
		return headSeqNo
	}
	conf := uint32(minConfirmations)
	if headSeqNo < conf-1 {
		return 0
	}
	return headSeqNo - conf + 1
}

// Freeze before the first remote call, including repeated connection failures.
func prepareTonBootstrap(cursor *tonRuntimeCursor) error {
	if cursor.persistent {
		saved, err := data.LoadPaymentCursor("ton:master")
		if err != nil {
			return err
		}
		if saved != "" {
			return nil
		}
	}
	_, err := data.EnsureNetworkScanStartTime(mdb.NetworkTon, time.Now())
	return err
}

func initializeTonRuntimeCursor(ctx context.Context, api ton.APIClientWrapped, head *ton.BlockIDExt, confirmedHeadSeqNo uint32, cursor *tonRuntimeCursor) error {
	if cursor == nil {
		return nil
	}
	if head == nil {
		return fmt.Errorf("current masterchain head is nil")
	}
	hasSaved := false
	if cursor.persistent {
		saved, err := data.LoadPaymentCursor("ton:master")
		if err != nil {
			return err
		}
		if saved != "" {
			n, err := strconv.ParseUint(saved, 10, 32)
			if err != nil {
				return err
			}
			confirmedHeadSeqNo = uint32(n)
			hasSaved = true
		}
	}
	if !hasSaved {
		start, err := data.EnsureNetworkScanStartTime(mdb.NetworkTon, time.Now())
		if err != nil {
			return err
		}
		// TON timestamps have second precision; include the entire boundary second.
		predecessor, err := paymentScanPredecessor(int64(confirmedHeadSeqNo), start.Truncate(time.Second), func(number int64) (int64, error) {
			block, err := api.WaitForBlock(uint32(number)).LookupBlock(ctx, address.MasterchainID, tonMasterShard, uint32(number))
			if err != nil {
				return 0, err
			}
			if block == nil || block.SeqNo != uint32(number) || block.Workchain != address.MasterchainID || block.Shard != tonMasterShard {
				return 0, fmt.Errorf("invalid TON bootstrap block %d", number)
			}
			contents, err := api.GetBlockHeader(ctx, block)
			if err != nil {
				return 0, err
			}
			if contents == nil || contents.SeqNo != uint32(number) {
				return 0, fmt.Errorf("invalid TON bootstrap block data %d", number)
			}
			return int64(contents.GenUtime) * 1000, nil
		})
		if err != nil {
			return fmt.Errorf("locate TON payment scan boundary: %w", err)
		}
		confirmedHeadSeqNo = uint32(predecessor)
	}
	initMaster := head
	if confirmedHeadSeqNo > 0 && confirmedHeadSeqNo != head.SeqNo {
		var err error
		initMaster, err = api.WaitForBlock(confirmedHeadSeqNo).LookupBlock(ctx, address.MasterchainID, tonMasterShard, confirmedHeadSeqNo)
		if err != nil {
			return fmt.Errorf("lookup initial confirmed masterchain seqno=%d: %w", confirmedHeadSeqNo, err)
		}
	}
	checkpoint := &tonRuntimeCursor{}
	if confirmedHeadSeqNo > 0 {
		if initMaster == nil || initMaster.SeqNo != confirmedHeadSeqNo || initMaster.Workchain != address.MasterchainID || initMaster.Shard != tonMasterShard {
			return fmt.Errorf("invalid initial TON masterchain block %d", confirmedHeadSeqNo)
		}
		if err := initializeTonShardCursors(ctx, api, initMaster, checkpoint); err != nil {
			return fmt.Errorf("initialize shard cursors masterchain seqno=%d: %w", initMaster.SeqNo, err)
		}
	}
	if cursor.persistent {
		if err := data.SavePaymentCursor("ton:master", strconv.FormatUint(uint64(confirmedHeadSeqNo), 10)); err != nil {
			return err
		}
	}
	cursor.shardSeqNo = checkpoint.shardSeqNo
	cursor.initialized = true
	cursor.lastMasterSeqNo = confirmedHeadSeqNo
	return nil
}

func processTonMasterBlock(ctx context.Context, api ton.APIClientWrapped, master *ton.BlockIDExt, state *tonWatchState, cache *tonJettonWalletCache) error {
	return processTonMasterBlockWithCursor(ctx, api, master, state, cache, nil)
}

func processTonMasterBlockWithCursor(ctx context.Context, api ton.APIClientWrapped, master *ton.BlockIDExt, state *tonWatchState, cache *tonJettonWalletCache, cursor *tonRuntimeCursor) error {
	if state.hasWalletInWorkchain(address.MasterchainID) {
		if err := processTonShardBlock(ctx, api, master, master, state, cache); err != nil {
			return fmt.Errorf("process masterchain transactions: %w", err)
		}
	}
	shards, err := api.WaitForBlock(master.SeqNo).GetBlockShardsInfo(ctx, master)
	if err != nil {
		return err
	}
	log.Sugar.Debugf(
		"[TON] masterchain seqno=%d root=%s file=%s shard_blocks=%d workchains=%s",
		master.SeqNo,
		shortTonHash(master.RootHash),
		shortTonHash(master.FileHash),
		len(shards),
		tonShardWorkchainSummary(shards),
	)
	if cursor != nil {
		return processTonShardBlocksWithCursor(ctx, api, master, shards, state, cache, cursor)
	}
	for _, shard := range shards {
		if err = processTonShardBlock(ctx, api, master, shard, state, cache); err != nil {
			return err
		}
	}
	return nil
}

func initializeTonShardCursors(ctx context.Context, api ton.APIClientWrapped, master *ton.BlockIDExt, cursor *tonRuntimeCursor) error {
	if cursor == nil || master == nil {
		return nil
	}
	shards, err := api.WaitForBlock(master.SeqNo).GetBlockShardsInfo(ctx, master)
	if err != nil {
		return err
	}
	cursor.ensureShardSeqNo()
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		cursor.shardSeqNo[tonShardCursorKey(shard)] = shard.SeqNo
	}
	log.Sugar.Infof(
		"[TON] initialized shard cursors masterchain seqno=%d shard_blocks=%d workchains=%s",
		master.SeqNo,
		len(shards),
		tonShardWorkchainSummary(shards),
	)
	return nil
}

func processTonShardBlocksWithCursor(ctx context.Context, api ton.APIClientWrapped, master *ton.BlockIDExt, shards []*ton.BlockIDExt, state *tonWatchState, cache *tonJettonWalletCache, cursor *tonRuntimeCursor) error {
	if cursor == nil {
		return nil
	}
	cursor.ensureShardSeqNo()
	// Sequence numbers belong to a shard lineage. Splits introduce new keys;
	// merges can resurrect an old key with a gap containing no such blocks.
	// Follow proven parent references across gaps instead of inventing numbers.
	boundaryWorkchains := make(map[string]bool)
	for checkpoint := range cursor.shardSeqNo {
		workchain, _, _ := strings.Cut(checkpoint, ":")
		boundaryWorkchains[workchain] = true
	}
	var visit func(*ton.BlockIDExt) error
	visit = func(block *ton.BlockIDExt) error {
		if block == nil {
			return fmt.Errorf("missing shard block")
		}
		key := tonShardCursorKey(block)
		last, known := cursor.shardSeqNo[key]
		if known && block.SeqNo <= last {
			return nil
		}
		if !state.hasWalletInWorkchain(block.Workchain) {
			cursor.shardSeqNo[key] = block.SeqNo
			return nil
		}
		// Empty cursors retain their existing initial-tip behavior. Production
		// initializes every shard at the persisted masterchain boundary first.
		hasBoundary := boundaryWorkchains[strconv.FormatInt(int64(block.Workchain), 10)]
		if hasBoundary && (!known || block.SeqNo != last+1) {
			// Lightweight header proofs can prune PrevRef in tonutils-go.
			// Full block data is hash-verified by the SDK and retains both merge
			// parents, so it cannot silently turn a pruned reference into zero.
			fullBlock, err := api.WaitForBlock(master.SeqNo).GetBlockData(ctx, block)
			if err != nil {
				return fmt.Errorf("load shard ancestry %s seqno=%d: %w", key, block.SeqNo, err)
			}
			if fullBlock == nil || fullBlock.BlockInfo.SeqNo != block.SeqNo {
				return fmt.Errorf("invalid shard ancestry header %s seqno=%d", key, block.SeqNo)
			}
			header := &fullBlock.BlockInfo
			wc, shard := tlb.ConvertShardIdentToShard(header.Shard)
			if wc != block.Workchain || int64(shard) != block.Shard {
				return fmt.Errorf("shard ancestry header identity mismatch %s", key)
			}
			parents, err := ton.GetParentBlocks(header)
			if err != nil {
				return err
			}
			for _, parent := range parents {
				// Strict descent also rejects cycles and incomplete parent data.
				if parent == nil || parent.Workchain != block.Workchain || parent.SeqNo >= block.SeqNo {
					return fmt.Errorf("invalid shard parent %s seqno=%d", key, block.SeqNo)
				}
				if err := visit(parent); err != nil {
					return err
				}
			}
		}
		if err := processTonShardBlock(ctx, api, master, block, state, cache); err != nil {
			return err
		}
		cursor.shardSeqNo[key] = block.SeqNo
		return nil
	}
	for _, shard := range shards {
		if shard != nil {
			if err := visit(shard); err != nil {
				return err
			}
		}
	}
	return nil
}

func processTonShardBlock(ctx context.Context, api ton.APIClientWrapped, master, shard *ton.BlockIDExt, state *tonWatchState, cache *tonJettonWalletCache) error {
	log.Sugar.Debugf("[TON] scanning shard master_seqno=%d workchain=%d shard=%d shard_seqno=%d", master.SeqNo, shard.Workchain, shard.Shard, shard.SeqNo)
	var after *ton.TransactionID3
	// A transaction proof does not establish pagination progress. Keep both
	// account and LT so repeated/cyclic pages fail through the normal retry path.
	type pageCursor struct {
		account string
		lt      uint64
	}
	seen := make(map[pageCursor]struct{})
	for {
		shorts, incomplete, err := api.WaitForBlock(master.SeqNo).GetBlockTransactionsV2(ctx, shard, tonBlockTxPageSize, after)
		if err != nil {
			return err
		}
		if len(shorts) == 0 && incomplete {
			return fmt.Errorf("incomplete transaction page without cursor progress workchain=%d shard=%d seqno=%d", shard.Workchain, shard.Shard, shard.SeqNo)
		}
		if len(shorts) > 0 {
			last := shorts[len(shorts)-1]
			key := pageCursor{account: string(last.Account), lt: last.LT}
			if _, repeated := seen[key]; repeated {
				return fmt.Errorf("repeated transaction page cursor workchain=%d shard=%d seqno=%d lt=%d", shard.Workchain, shard.Shard, shard.SeqNo, last.LT)
			}
			seen[key] = struct{}{}
		}
		for i := range shorts {
			account := addressutil.TonAddressFromShortAccount(shard.Workchain, shorts[i].Account)
			if account == nil {
				continue
			}
			receive, ok := state.wallets[addressutil.TonRawAddressObjectKey(account)]
			if !ok {
				continue
			}
			log.Sugar.Debugf("[TON] watched account tx candidate master_seqno=%d workchain=%d shard=%d lt=%d account=%s", master.SeqNo, shard.Workchain, shard.Shard, shorts[i].LT, addressutil.TonRawAddressObjectKey(account))
			tx, err := api.WaitForBlock(master.SeqNo).GetTransaction(ctx, shard, account, shorts[i].LT)
			if err != nil {
				return err
			}
			if tx == nil {
				return fmt.Errorf("missing full transaction workchain=%d shard=%d seqno=%d lt=%d", shard.Workchain, shard.Shard, shard.SeqNo, shorts[i].LT)
			}
			jettonWallets, err := cache.jettonWalletTokens(ctx, api, master, receive, state.jettonTokens)
			if err != nil {
				return err
			}
			transfer, err := service.ParseTonInboundTransfer(tx, receive, state.nativeToken, jettonWallets)
			if err != nil {
				log.Sugar.Warnf("[TON] parse inbound tx lt=%d hash=%s: %v", tx.LT, hex.EncodeToString(tx.Hash), err)
				continue
			}
			if transfer == nil {
				log.Sugar.Debugf(
					"[TON] unsupported watched tx master_seqno=%d workchain=%d shard=%d lt=%d hash=%s account=%s",
					master.SeqNo,
					shard.Workchain,
					shard.Shard,
					tx.LT,
					hex.EncodeToString(tx.Hash),
					addressutil.TonRawAddressObjectKey(account),
				)
				continue
			}
			if err := service.ProcessTonTransfer(transfer); err != nil {
				return err
			}
		}
		if !incomplete {
			return nil
		}
		after = shorts[len(shorts)-1].ID3()
	}
}

func (c *tonJettonWalletCache) jettonWalletTokens(ctx context.Context, api ton.APIClientWrapped, master *ton.BlockIDExt, owner *address.Address, tokens []mdb.ChainToken) (map[string]mdb.ChainToken, error) {
	out := make(map[string]mdb.ChainToken, len(tokens))
	ownerRaw := addressutil.TonRawAddressObjectKey(owner)
	for _, token := range tokens {
		masterParsed, err := addressutil.ParseTonMainnetAddress(token.ContractAddress)
		if err != nil {
			return nil, fmt.Errorf("invalid jetton master symbol=%s address=%s: %w", token.Symbol, token.ContractAddress, err)
		}
		cacheKey := masterParsed.Raw + "|" + ownerRaw
		c.Lock()
		walletRaw, ok := c.walletByOwnerToken[cacheKey]
		c.Unlock()
		if !ok {
			wallet, err := jetton.NewJettonMasterClient(api, masterParsed.Address).GetJettonWalletAtBlock(ctx, owner, master)
			if err != nil {
				return nil, fmt.Errorf("derive jetton wallet symbol=%s owner=%s: %w", token.Symbol, ownerRaw, err)
			}
			walletRaw = addressutil.TonRawAddressObjectKey(wallet.Address())
			c.Lock()
			c.walletByOwnerToken[cacheKey] = walletRaw
			c.Unlock()
		}
		out[walletRaw] = token
	}
	return out, nil
}

func loadTonWatchState() (*tonWatchState, error) {
	walletRows, err := data.GetAvailableWalletAddressByNetwork(mdb.NetworkTon)
	if err != nil {
		return nil, err
	}
	tokens, err := data.ListEnabledChainTokensByNetwork(mdb.NetworkTon)
	if err != nil {
		return nil, err
	}
	state := &tonWatchState{wallets: make(map[string]*address.Address)}
	for _, row := range walletRows {
		parsed, err := addressutil.ParseTonMainnetAddress(row.Address)
		if err != nil {
			log.Sugar.Warnf("[TON] skip invalid wallet address=%s err=%v", row.Address, err)
			continue
		}
		state.wallets[parsed.Raw] = parsed.Address
	}
	for i := range tokens {
		sym := strings.ToUpper(strings.TrimSpace(tokens[i].Symbol))
		contract := strings.TrimSpace(tokens[i].ContractAddress)
		switch {
		case sym == service.TonNativeSymbol && contract == "":
			token := tokens[i]
			state.nativeToken = &token
		case contract != "":
			state.jettonTokens = append(state.jettonTokens, tokens[i])
		}
	}
	return state, nil
}

func tonChainConfig() (*mdb.Chain, time.Duration) {
	row, err := data.GetChainByNetwork(mdb.NetworkTon)
	if err != nil || row.ID == 0 {
		return nil, 5 * time.Second
	}
	interval := time.Duration(row.ScanIntervalSec) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return row, interval
}

func tonWatchStateSignature(state *tonWatchState) string {
	if state == nil {
		return ""
	}
	return fmt.Sprintf("%d|%t|%d|%s", len(state.wallets), state.nativeToken != nil, len(state.jettonTokens), tonWalletWorkchainSummary(state))
}

func tonWalletWorkchainSummary(state *tonWatchState) string {
	if state == nil || len(state.wallets) == 0 {
		return "none"
	}
	counts := make(map[int32]int)
	for _, wallet := range state.wallets {
		if wallet == nil {
			continue
		}
		counts[wallet.Workchain()]++
	}
	return formatTonWorkchainCounts(counts)
}

func tonShardWorkchainSummary(shards []*ton.BlockIDExt) string {
	if len(shards) == 0 {
		return "none"
	}
	counts := make(map[int32]int)
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		counts[shard.Workchain]++
	}
	return formatTonWorkchainCounts(counts)
}

func tonShardCursorKey(shard *ton.BlockIDExt) string {
	if shard == nil {
		return ""
	}
	return fmt.Sprintf("%d:%d", shard.Workchain, shard.Shard)
}

func formatTonWorkchainCounts(counts map[int32]int) string {
	if len(counts) == 0 {
		return "none"
	}
	workchains := make([]int, 0, len(counts))
	for workchain := range counts {
		workchains = append(workchains, int(workchain))
	}
	sort.Ints(workchains)
	parts := make([]string, 0, len(workchains))
	for _, workchain := range workchains {
		parts = append(parts, fmt.Sprintf("%d:%d", workchain, counts[int32(workchain)]))
	}
	return strings.Join(parts, ",")
}

func shortTonHash(raw []byte) string {
	hash := hex.EncodeToString(raw)
	if len(hash) > 16 {
		return hash[:16]
	}
	if hash == "" {
		return "-"
	}
	return hash
}

func resolveTonLiteNode(excludeIDs ...uint64) (mdb.RpcNode, bool) {
	node, err := data.SelectGeneralRpcNode(mdb.NetworkTon, mdb.RpcNodeTypeLite, excludeIDs...)
	if err == nil && node != nil && node.ID > 0 {
		node.Url = strings.TrimSpace(node.Url)
		if node.Url != "" {
			return *node, true
		}
		log.Sugar.Errorf("[TON] rpc_nodes id=%d has empty url", node.ID)
		return mdb.RpcNode{}, false
	}
	if err != nil {
		log.Sugar.Errorf("[TON] resolve lite rpc_nodes err=%v", err)
	} else {
		log.Sugar.Warn("[TON] no enabled lite RPC node configured in rpc_nodes")
	}
	return mdb.RpcNode{}, false
}

func connectTonLiteAPI(node mdb.RpcNode) (ton.APIClientWrapped, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), tonScannerConnectTimeout)
	defer cancel()
	return service.ConnectTonLiteAPI(ctx, node.Url, tonScannerRequestTimeout, 5)
}

func recordTonLiteNodeFailure(node mdb.RpcNode, reason string) {
	data.RecordRpcFailure(mdb.NetworkTon)
	failures, cooling := data.RecordRpcNodeFailure(node.ID)
	nodeLabel := data.RpcNodeLogLabel(node)
	if cooling {
		log.Sugar.Warnf("[TON] lite node reached fail threshold (%s), node=%s failures=%d/%d", reason, nodeLabel, failures, data.RpcFailoverThreshold)
		return
	}
	log.Sugar.Warnf("[TON] lite node failed (%s), node=%s failures=%d/%d", reason, nodeLabel, failures, data.RpcFailoverThreshold)
}
