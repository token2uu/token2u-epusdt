package task

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"
)

type ListenSolJob struct{}

var gListenSolJobLock sync.Mutex
var gSolanaBlockHeightLock = make(chan struct{}, 1)

// Accessed under gListenSolJobLock. A restart makes every enabled address due;
// durable signature cursors still determine where each scan resumes.
var solanaSchedule = newSolanaScanSchedule()

type solanaScanSchedule struct {
	now        func() time.Time
	lastScan   map[string]time.Time
	lastHeight time.Time
}

func newSolanaScanSchedule() *solanaScanSchedule {
	return &solanaScanSchedule{now: time.Now, lastScan: make(map[string]time.Time)}
}

func (r ListenSolJob) Run() {
	shouldRecordHeight := false
	func() {
		// A slow scan must not accumulate another waiting job on each tick.
		// The next admitted scan resumes from the persisted payment cursors.
		if !gListenSolJobLock.TryLock() {
			log.Sugar.Debug("[ListenSolJob] previous scan still running, skipping")
			return
		}
		defer gListenSolJobLock.Unlock()

		log.Sugar.Debug("[ListenSolJob] Job triggered")
		if !data.IsChainEnabled(mdb.NetworkSolana) {
			log.Sugar.Debug("[ListenSolJob] chain disabled, skipping")
			return
		}
		walletAddress, err := data.GetAvailableWalletAddressByNetwork(mdb.NetworkSolana)
		if err != nil {
			log.Sugar.Errorf("[ListenSolJob] Failed to get wallet addresses: %v", err)
			return
		}
		if len(walletAddress) <= 0 {
			clear(solanaSchedule.lastScan)
			log.Sugar.Debug("[ListenSolJob] No available wallet addresses")
			return
		}
		now := solanaSchedule.now()
		active, activityErr := data.RecentlyActivePaymentAddresses(mdb.NetworkSolana, now, config.GetSolanaRecentActivityWindow())
		if activityErr != nil {
			// A failed activity lookup must not cause missed payments.
			log.Sugar.Warnf("[ListenSolJob] activity lookup failed, scanning all enabled addresses: %v", activityErr)
		}
		due := make([]string, 0, len(walletAddress))
		enabled := make(map[string]bool, len(walletAddress))
		for _, wallet := range walletAddress {
			address := strings.TrimSpace(wallet.Address)
			if enabled[address] { // Duplicate rows must not duplicate RPC work.
				continue
			}
			enabled[address] = true
			last, seen := solanaSchedule.lastScan[address]
			if activityErr == nil && !active[address] && seen && data.PaidRpcBudgetExceeded(mdb.NetworkSolana) {
				continue
			}
			if activityErr != nil || active[address] || !seen || now.Sub(last) >= config.GetSolanaIdleScanInterval() {
				due = append(due, address)
				solanaSchedule.lastScan[address] = now
			}
		}
		for address := range solanaSchedule.lastScan {
			if !enabled[address] {
				delete(solanaSchedule.lastScan, address)
			}
		}
		if len(due) == 0 {
			return
		}
		if solanaSchedule.lastHeight.IsZero() || now.Sub(solanaSchedule.lastHeight) >= max(time.Minute, config.GetSolanaScanInterval()) {
			shouldRecordHeight = true
			solanaSchedule.lastHeight = now
		}
		log.Sugar.Infof("[ListenSolJob] scanning %d/%d enabled addresses", len(due), len(enabled))
		var wg sync.WaitGroup
		results := make(chan error, len(due))
		permits := make(chan struct{}, config.GetSolanaScanConcurrency())
		for _, address := range due {
			wg.Add(1)
			permits <- struct{}{}
			go func(addr string) {
				defer wg.Done()
				defer func() { <-permits }()
				results <- service.ScanSolanaAddress(addr)
			}(address)
		}
		wg.Wait()
		close(results)
		var failures []error
		for err := range results {
			if err != nil {
				failures = append(failures, err)
			}
		}
		data.RecordRpcScanResult(mdb.NetworkSolana, errors.Join(failures...))
		log.Sugar.Debug("[ListenSolJob] Job completed")
	}()

	if shouldRecordHeight {
		recordSolanaLatestBlockHeight()
	}
}

func recordSolanaLatestBlockHeight() {
	select {
	case gSolanaBlockHeightLock <- struct{}{}:
		defer func() { <-gSolanaBlockHeightLock }()
	default:
		log.Sugar.Debug("[ListenSolJob] previous Solana block height stats still running, skipping")
		return
	}

	height, err := service.SolGetBlockHeight()
	if err != nil {
		log.Sugar.Warnf("[ListenSolJob] get Solana block height failed: %v", err)
	} else {
		data.RecordRpcBlockHeight(mdb.NetworkSolana, height)
	}
}
