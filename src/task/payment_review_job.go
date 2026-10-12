package task

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"
)

// Each network has a separate job; a slow indexer cannot block Solana reviews.
// Reviews have separate cursors from ordinary scans and a bounded lifetime.
type PaymentReviewJob struct {
	Network     string
	mu          sync.Mutex
	lastAttempt map[string]uint64
	sequence    uint64
}

func (j *PaymentReviewJob) Run() {
	if !j.mu.TryLock() {
		return
	}
	defer j.mu.Unlock()
	if !data.IsChainEnabled(j.Network) {
		return
	}
	orders, err := data.ListPaymentReviewOrders(j.Network, time.Now())
	if err != nil {
		log.Sugar.Errorf("[PAYMENT-REVIEW] load orders network=%s: %v", j.Network, err)
		return
	}
	// Finish before the next 30-second trigger. Rotate addresses across rounds
	// so repeated failures cannot monopolize the start of every queue.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	j.runAddresses(ctx, orders, func(ctx context.Context, address string) error {
		ctx = service.WithPaidPaymentReview(ctx)
		switch j.Network {
		case mdb.NetworkSolana:
			return service.ScanSolanaAddressWithContext(ctx, address)
		case mdb.NetworkAptos:
			return reviewAptosPayment(ctx, address)
		}
		return nil
	})
}

func (j *PaymentReviewJob) runAddresses(ctx context.Context, orders []mdb.Orders, scan func(context.Context, string) error) {
	seen := make(map[string]bool)
	addresses := make([]string, 0, len(orders))
	for _, order := range orders {
		if !seen[order.ReceiveAddress] {
			seen[order.ReceiveAddress] = true
			addresses = append(addresses, order.ReceiveAddress)
		}
	}
	if j.lastAttempt == nil {
		j.lastAttempt = make(map[string]uint64)
	}
	for address := range j.lastAttempt {
		if !seen[address] {
			delete(j.lastAttempt, address)
		}
	}
	sort.SliceStable(addresses, func(i, k int) bool {
		return j.lastAttempt[addresses[i]] < j.lastAttempt[addresses[k]]
	})
	var wg sync.WaitGroup
	defer wg.Wait()
	permits := make(chan struct{}, 2)
	for _, address := range addresses {
		select {
		case <-ctx.Done():
			return
		case permits <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-permits
			return
		}
		j.sequence++
		j.lastAttempt[address] = j.sequence
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			defer func() { <-permits }()
			addressCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
			defer cancel()
			if err := scan(addressCtx, address); err != nil {
				log.Sugar.Warnf("[PAYMENT-REVIEW] incomplete network=%s address=%s: %v", j.Network, address, err)
			} else {
				log.Sugar.Infof("[PAYMENT-REVIEW] completed network=%s address=%s", j.Network, address)
			}
		}(address)
	}
}

func reviewAptosPayment(ctx context.Context, owner string) error {
	if _, loaded := aptosReviewsInFlight.LoadOrStore(owner, struct{}{}); loaded {
		return fmt.Errorf("Aptos payment review already running for %s", owner)
	}
	defer aptosReviewsInFlight.Delete(owner)
	chain, _ := moveChainConfig(mdb.NetworkAptos)
	provider := serviceAptosDepositProvider{ctx: ctx}
	// Freeze the boundary even when the independent paid indexer is unavailable.
	if _, err := aptosDepositCursorWithContext(ctx, owner); err != nil {
		return err
	}
	work, err := loadAptosReviewWork(ctx, owner)
	if err != nil {
		return err
	}
	var head int64
	if work != nil {
		head = work.Head
	} else {
		indexed, err := provider.Head(ctx)
		if err != nil {
			return err
		}
		confirmations := 1
		if chain != nil {
			confirmations = chain.MinConfirmations
		}
		head = moveConfirmedCursor(indexed, confirmations)
		if err := saveAptosReviewWork(ctx, owner, &aptosReviewWork{Version: 1, Head: head}); err != nil {
			return err
		}
	}
	tokens, err := data.ListEnabledChainTokensByNetwork(mdb.NetworkAptos)
	if err != nil {
		return err
	}
	return scanAptosOwnerDeposits(ctx, provider, owner, tokens, head)
}
