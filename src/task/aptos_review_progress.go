package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/service"
)

var aptosReviewsInFlight sync.Map

// Save each remote stage independently. A slow head query or page fetch must
// not consume the next round's budget again before transaction verification.
type aptosReviewWork struct {
	Version   int
	Head      int64
	PageReady bool
	After     service.AptosEventPosition
	Rows      []service.AptosIndexedDeposit
}

func loadAptosReviewWork(ctx context.Context, owner string) (*aptosReviewWork, error) {
	stored, err := data.LoadPaymentCursor(aptosDepositKey(ctx, owner) + ":work")
	if err != nil || stored == "" {
		return nil, err
	}
	var work aptosReviewWork
	if err := json.Unmarshal([]byte(stored), &work); err != nil {
		return nil, fmt.Errorf("invalid Aptos review checkpoint: %w", err)
	}
	if work.Version != 1 || work.Head < 0 || (!work.PageReady && len(work.Rows) > 0) {
		return nil, fmt.Errorf("invalid Aptos review checkpoint")
	}
	if work.PageReady {
		if work.After.Version < -1 || work.After.Index < 0 {
			return nil, fmt.Errorf("invalid Aptos review page boundary")
		}
		if err := validateAptosDepositPage(work.Rows, owner, work.After, work.Head); err != nil {
			return nil, err
		}
	}
	return &work, nil
}

func saveAptosReviewWork(ctx context.Context, owner string, work *aptosReviewWork) error {
	encoded, err := json.Marshal(work)
	if err != nil {
		return err
	}
	return data.SavePaymentCursor(aptosDepositKey(ctx, owner)+":work", string(encoded))
}

func clearAptosReviewWork(ctx context.Context, owner string) error {
	return data.SavePaymentCursor(aptosDepositKey(ctx, owner)+":work", "")
}
