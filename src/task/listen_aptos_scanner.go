package task

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/spf13/viper"
)

const (
	aptosLedgerChunkSize   int64 = 100
	aptosLedgerWorkerCount       = 3
	aptosLedgerQueueLimit        = 100
)

type aptosRuntimeCursor struct {
	persistent      bool
	initialized     bool
	lastSeenVersion int64
}

type aptosLedgerRange struct {
	start int64
	limit int64
	state moveWatchState
}

type aptosChainProvider interface {
	LatestLedgerVersion() (int64, error)
	Transactions(start int64, limit int64) ([]byte, error)
}

type serviceAptosProvider struct{}

func (serviceAptosProvider) LatestLedgerVersion() (int64, error) {
	return service.AptosGetLedgerVersion()
}

func (serviceAptosProvider) Transactions(start int64, limit int64) ([]byte, error) {
	return service.AptosGetTransactions(start, limit)
}

var (
	parseAptosTransfersForWallets    = service.ParseAptosTransfersForWallets
	processAptosObservedTransferFunc = service.ProcessMoveObservedTransferResult
)

func StartAptosLedgerScannerListener() {
	// Full-ledger scanning is an explicit rollback mode, never an automatic
	// fallback from an unavailable indexer (which could spend unbounded credits).
	if viper.GetString("aptos_scan_mode") != "full_ledger" {
		startAptosAddressScanner()
		return
	}
	provider := serviceAptosProvider{}
	cursor := &aptosRuntimeCursor{persistent: true}
	log.Sugar.Infof("[APTOS] ledger scanner starting mode=full_ledger chunk_size=%d workers=%d queue_limit=%d",
		aptosLedgerChunkSize,
		aptosLedgerWorkerCount,
		aptosLedgerQueueLimit,
	)
	for {
		if err := runAptosLedgerScanner(context.Background(), provider, cursor); err != nil {
			log.Sugar.Warnf("[APTOS] scanner stopped: %v", err)
			sleepOrDone(context.Background(), moveScannerRetryDelay)
			continue
		}
	}
}

func runAptosLedgerScanner(ctx context.Context, provider aptosChainProvider, cursor *aptosRuntimeCursor) error {
	if cursor == nil {
		cursor = &aptosRuntimeCursor{}
	}
	watchSignature := ""
	for {
		chain, interval := moveChainConfig(mdb.NetworkAptos)
		if chain == nil || !chain.Enabled {
			log.Sugar.Debug("[APTOS] chain disabled or not configured, idling")
			if !sleepOrDone(ctx, interval) {
				return nil
			}
			continue
		}
		state, err := loadMoveWatchState(mdb.NetworkAptos)
		if err != nil {
			return err
		}
		if len(state.wallets) == 0 || len(state.tokens) == 0 {
			log.Sugar.Debug("[APTOS] no enabled wallets or tokens, idling")
			if !sleepOrDone(ctx, interval) {
				return nil
			}
			continue
		}
		if sig := moveWatchStateSignature(state); sig != watchSignature {
			watchSignature = sig
			log.Sugar.Infof("[APTOS] ledger scanner watching wallets=%d tokens=%d", len(state.wallets), len(state.tokens))
		}

		latest, err := provider.LatestLedgerVersion()
		if err != nil {
			return fmt.Errorf("get latest ledger version: %w", err)
		}
		data.RecordRpcBlockHeight(mdb.NetworkAptos, latest)
		confirmed := moveConfirmedCursor(latest, chain.MinConfirmations)
		if !cursor.initialized && cursor.persistent {
			saved, err := data.LoadPaymentCursor("aptos:ledger")
			if err != nil {
				return err
			}
			if saved != "" {
				cursor.lastSeenVersion, err = strconv.ParseInt(saved, 10, 64)
				if err != nil {
					return err
				}
				cursor.initialized = true
			}
		}
		if !cursor.initialized {
			cursor.initialized = true
			cursor.lastSeenVersion = confirmed - aptosLedgerChunkSize
			if cursor.lastSeenVersion < -1 {
				cursor.lastSeenVersion = -1
			}
			if cursor.persistent {
				if err := data.SavePaymentCursor("aptos:ledger", strconv.FormatInt(cursor.lastSeenVersion, 10)); err != nil {
					cursor.initialized = false
					return err
				}
			}
			log.Sugar.Infof(
				"[APTOS] initialized ledger cursor at version=%d confirmed_head=%d chunk_size=%d mode=full_ledger_watch",
				cursor.lastSeenVersion,
				confirmed,
				aptosLedgerChunkSize,
			)
		}

		catchup, err := processAptosLedgerRound(ctx, provider, state, cursor, confirmed)
		if err != nil {
			return err
		}
		if !sleepOrDone(ctx, chooseAptosIdleDelay(catchup, interval)) {
			return nil
		}
	}
}

func processAptosLedgerRound(ctx context.Context, provider aptosChainProvider, state moveWatchState, cursor *aptosRuntimeCursor, confirmedVersion int64) (bool, error) {
	ranges := buildAptosLedgerRanges(cursor.lastSeenVersion, confirmedVersion, state)
	if len(ranges) == 0 {
		return false, nil
	}
	lastRange := ranges[len(ranges)-1]
	log.Sugar.Infof(
		"[APTOS] queued ledger ranges count=%d from=%d to=%d cursor=%d confirmed_head=%d workers=%d",
		len(ranges),
		ranges[0].start,
		lastRange.start+lastRange.limit-1,
		cursor.lastSeenVersion,
		confirmedVersion,
		aptosLedgerWorkerCount,
	)

	type rangeResult struct {
		item aptosLedgerRange
		err  error
	}
	jobs := make(chan aptosLedgerRange, len(ranges))
	results := make(chan rangeResult, len(ranges))
	workers := aptosLedgerWorkerCount
	if len(ranges) < workers {
		workers = len(ranges)
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				results <- rangeResult{item: item, err: processAptosLedgerRange(ctx, provider, item, confirmedVersion)}
			}
		}()
	}
	for _, item := range ranges {
		jobs <- item
	}
	close(jobs)
	wg.Wait()
	close(results)

	errByStart := make(map[int64]error, len(ranges))
	for result := range results {
		if result.err != nil {
			errByStart[result.item.start] = result.err
		}
	}

	for _, item := range ranges {
		if err := errByStart[item.start]; err != nil {
			return cursor.lastSeenVersion < confirmedVersion, err
		}
		next := item.start + item.limit - 1
		if cursor.persistent {
			if err := data.SavePaymentCursor("aptos:ledger", strconv.FormatInt(next, 10)); err != nil {
				return true, err
			}
		}
		cursor.lastSeenVersion = next
	}
	return cursor.lastSeenVersion < confirmedVersion, nil
}

func buildAptosLedgerRanges(lastSeenVersion int64, confirmedVersion int64, state moveWatchState) []aptosLedgerRange {
	if confirmedVersion <= lastSeenVersion {
		return nil
	}
	ranges := make([]aptosLedgerRange, 0, aptosLedgerQueueLimit)
	next := lastSeenVersion + 1
	if next < 0 {
		next = 0
	}
	for next <= confirmedVersion && len(ranges) < aptosLedgerQueueLimit {
		limit := confirmedVersion - next + 1
		if limit > aptosLedgerChunkSize {
			limit = aptosLedgerChunkSize
		}
		ranges = append(ranges, aptosLedgerRange{start: next, limit: limit, state: state})
		next += limit
	}
	return ranges
}

func processAptosLedgerRange(ctx context.Context, provider aptosChainProvider, item aptosLedgerRange, confirmedVersion int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := provider.Transactions(item.start, item.limit)
	if err != nil {
		return fmt.Errorf("get transactions start=%d limit=%d: %w", item.start, item.limit, err)
	}
	// Validate the whole ledger range before applying any payment. A successful
	// HTTP response (including [] or a short page) does not prove full coverage.
	if err := validateAptosLedgerBatch(body, item.start, item.limit); err != nil {
		return fmt.Errorf("validate transactions start=%d limit=%d: %w", item.start, item.limit, err)
	}
	transfers, err := parseAptosTransfersForWallets(body, item.state.wallets, item.state.tokens)
	if err != nil {
		return fmt.Errorf("parse transactions start=%d limit=%d: %w", item.start, item.limit, err)
	}
	for _, transfer := range transfers {
		log.Sugar.Infof(
			"[APTOS] observed transfer version=%d tx=%s token=%s amount=%.8f raw=%s decimals=%d receive=%s key=%s",
			transfer.Version,
			transfer.TxID,
			transfer.Token,
			transfer.Amount,
			transfer.RawAmount.String(),
			transfer.Decimals,
			transfer.ReceiveAddress,
			transfer.TransferKey,
		)
		if err = processAptosObservedTransferFunc(transfer); err != nil {
			return err
		}
	}
	if len(transfers) > 0 {
		log.Sugar.Infof(
			"[APTOS] ledger range processed start=%d limit=%d to=%d transfers=%d confirmed_head=%d",
			item.start,
			item.limit,
			item.start+item.limit-1,
			len(transfers),
			confirmedVersion,
		)
	}
	return nil
}

func validateAptosLedgerBatch(body []byte, start, limit int64) error {
	// Check all committed transaction types, not only successful user transfers:
	// block metadata, checkpoints and failed transactions also occupy versions.
	var transactions []struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &transactions); err != nil {
		return fmt.Errorf("invalid transaction array: %w", err)
	}
	if int64(len(transactions)) != limit {
		return fmt.Errorf("incomplete ledger range: got %d transactions, want %d", len(transactions), limit)
	}
	for i, transaction := range transactions {
		// Aptos REST encodes u64 versions as strings. Avoid floating-point
		// conversion so large versions cannot silently lose integer precision.
		version, err := strconv.ParseUint(transaction.Version, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid version at index %d: %w", i, err)
		}
		if version != uint64(start)+uint64(i) {
			return fmt.Errorf("non-contiguous ledger range at index %d: got version %d, want %d", i, version, uint64(start)+uint64(i))
		}
	}
	return validateAptosLedgerContents(body)
}

func chooseAptosIdleDelay(catchup bool, interval time.Duration) time.Duration {
	if catchup {
		return moveCatchupYield
	}
	return interval
}
