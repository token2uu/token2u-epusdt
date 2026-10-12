package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"time"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/tidwall/gjson"
)

const aptosDepositPageSize = 100
const aptosDepositPagesPerRound = 10

type aptosDepositProvider interface {
	Head(context.Context) (int64, error)
	Deposits(context.Context, string, service.AptosEventPosition, int64, int, string) ([]service.AptosIndexedDeposit, error)
	Transaction(int64, func([]byte) error) ([]byte, error)
}

type serviceAptosDepositProvider struct{ ctx context.Context }

func (serviceAptosDepositProvider) Head(ctx context.Context) (int64, error) {
	return service.AptosIndexerHead(ctx)
}
func (serviceAptosDepositProvider) Deposits(ctx context.Context, owner string, after service.AptosEventPosition, head int64, limit int, since string) ([]service.AptosIndexedDeposit, error) {
	return service.AptosIndexerDepositsSince(ctx, owner, after, head, limit, since)
}
func (p serviceAptosDepositProvider) Transaction(version int64, validate func([]byte) error) ([]byte, error) {
	if p.ctx != nil {
		return service.AptosGetTransactionByVersionWithContext(p.ctx, version, validate)
	}
	return service.AptosGetTransactionByVersionValidated(version, validate)
}

func startAptosAddressScanner() {
	ctx := context.Background()
	provider := serviceAptosDepositProvider{}
	log.Sugar.Info("[APTOS] scanner starting mode=address_events; RPC verifies indexed deposits only")
	for {
		chain, interval := moveChainConfig(mdb.NetworkAptos)
		if chain != nil && chain.Enabled {
			state, err := loadMoveWatchState(mdb.NetworkAptos)
			if err == nil && len(state.wallets) > 0 && len(state.tokens) > 0 {
				err = scanAptosDepositRound(ctx, provider, state, chain.MinConfirmations)
			}
			if err != nil {
				log.Sugar.Warnf("[APTOS] address scan incomplete; retaining cursors: %v", err)
				interval = max(interval, 30*time.Second)
			}
		}
		sleepOrDone(ctx, interval)
	}
}

func scanAptosDepositRound(ctx context.Context, provider aptosDepositProvider, state moveWatchState, confirmations int) (roundErr error) {
	defer func() { data.RecordRpcScanResult(mdb.NetworkAptos, roundErr) }()
	// Freeze every new address's boundary before the first network request,
	// including when the indexer is unavailable or the paid budget is exhausted.
	var failures []error
	unavailable := make(map[string]bool)
	for owner := range state.wallets {
		if _, err := aptosDepositCursor(owner); err != nil {
			failures = append(failures, fmt.Errorf("owner=%s: %w", owner, err))
			unavailable[owner] = true
		}
	}
	indexed, err := provider.Head(ctx)
	if err != nil {
		return errors.Join(append(failures, err)...)
	}
	if indexed <= 0 {
		return fmt.Errorf("invalid indexed ledger head")
	}
	data.RecordRpcBlockHeight(mdb.NetworkAptos, indexed)
	head := moveConfirmedCursor(indexed, confirmations)
	owners := make([]string, 0, len(state.wallets))
	for owner := range state.wallets {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	active, activityErr := data.RecentlyActivePaymentAddresses(mdb.NetworkAptos, time.Now(), 10*time.Minute)
	deferCold := data.PaidRpcBudgetExceeded(mdb.NetworkAptos) && activityErr == nil
	for _, owner := range owners {
		if unavailable[owner] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if deferCold && !active[owner] {
			continue
		}
		if err := scanAptosOwnerDeposits(ctx, provider, owner, state.tokens, head); err != nil {
			failures = append(failures, fmt.Errorf("owner=%s: %w", owner, err))
		}
	}
	return errors.Join(failures...)
}

func aptosDepositCursor(owner string) (service.AptosEventPosition, error) {
	return aptosDepositCursorWithContext(context.Background(), owner)
}

func aptosDepositKey(ctx context.Context, owner string) string {
	if service.IsPaidPaymentReview(ctx) {
		return "aptos:payment_review:" + owner
	}
	return "aptos:deposits:" + owner
}

func aptosDepositCursorWithContext(ctx context.Context, owner string) (service.AptosEventPosition, error) {
	key := aptosDepositKey(ctx, owner)
	stored, err := data.LoadPaymentCursor(key)
	if err != nil {
		return service.AptosEventPosition{}, err
	}
	if stored != "" {
		var fields struct {
			Version *int64 `json:"version"`
			Index   *int64 `json:"index"`
		}
		if err := json.Unmarshal([]byte(stored), &fields); err != nil || fields.Version == nil || fields.Index == nil || *fields.Version < -1 || *fields.Index < 0 {
			return service.AptosEventPosition{}, fmt.Errorf("invalid Aptos deposit cursor")
		}
		return service.AptosEventPosition{Version: *fields.Version, Index: *fields.Index}, nil
	}
	// Migrate each address from the last fully processed legacy ledger version.
	// Do not initialize at the latest head: an existing payment may be behind it.
	legacy, err := data.LoadPaymentCursor("aptos:ledger")
	if err != nil {
		return service.AptosEventPosition{}, err
	}
	if service.IsPaidPaymentReview(ctx) {
		legacy = ""
	}
	version := int64(-1)
	if legacy != "" {
		version, err = strconv.ParseInt(legacy, 10, 64)
		if err != nil || version < -1 {
			return service.AptosEventPosition{}, fmt.Errorf("invalid legacy Aptos ledger cursor")
		}
	} else {
		startKey := key + ":start_time"
		start, err := data.LoadPaymentCursor(startKey)
		if err != nil {
			return service.AptosEventPosition{}, err
		}
		if start == "" {
			boundary, err := data.PaymentScanStartTime(mdb.NetworkAptos, owner, time.Now())
			if err != nil {
				return service.AptosEventPosition{}, err
			}
			if err := data.SavePaymentCursor(startKey, boundary.UTC().Format(time.RFC3339Nano)); err != nil {
				return service.AptosEventPosition{}, err
			}
		}
	}
	cursor := service.AptosEventPosition{Version: version, Index: math.MaxInt64}
	return cursor, saveAptosDepositCursorWithContext(ctx, owner, cursor)
}

func saveAptosDepositCursor(owner string, cursor service.AptosEventPosition) error {
	return saveAptosDepositCursorWithContext(context.Background(), owner, cursor)
}

func saveAptosDepositCursorWithContext(ctx context.Context, owner string, cursor service.AptosEventPosition) error {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	return data.SavePaymentCursor(aptosDepositKey(ctx, owner), string(encoded))
}

func scanAptosOwnerDeposits(ctx context.Context, provider aptosDepositProvider, owner string, tokens []mdb.ChainToken, head int64) error {
	cursor, err := aptosDepositCursorWithContext(ctx, owner)
	if err != nil {
		return err
	}
	var work *aptosReviewWork
	if service.IsPaidPaymentReview(ctx) {
		work, err = loadAptosReviewWork(ctx, owner)
		if err != nil {
			return err
		}
		if work == nil {
			work = &aptosReviewWork{Version: 1, Head: head}
			if err := saveAptosReviewWork(ctx, owner, work); err != nil {
				return err
			}
		}
		head = work.Head
	}
	end := service.AptosEventPosition{Version: head, Index: math.MaxInt64}
	if !end.After(cursor) { // An indexer rollback/lag must never rewind progress.
		if work != nil {
			return clearAptosReviewWork(ctx, owner)
		}
		return nil
	}
	since, err := data.LoadPaymentCursor(aptosDepositKey(ctx, owner) + ":start_time")
	if err != nil {
		return err
	}
	if since != "" {
		if _, err := time.Parse(time.RFC3339Nano, since); err != nil {
			return fmt.Errorf("invalid Aptos scan start time")
		}
	}
	for page := 0; page < aptosDepositPagesPerRound; page++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var rows []service.AptosIndexedDeposit
		if work != nil && work.PageReady {
			if work.After.After(cursor) {
				return fmt.Errorf("Aptos review checkpoint is ahead of committed cursor")
			}
			rows = work.Rows
		} else {
			rows, err = provider.Deposits(ctx, owner, cursor, head, aptosDepositPageSize, since)
			if err != nil {
				return err
			}
			if err := validateAptosDepositPage(rows, owner, cursor, head); err != nil {
				return err
			}
			if work != nil {
				work.PageReady, work.After, work.Rows = true, cursor, rows
				if err := saveAptosReviewWork(ctx, owner, work); err != nil {
					return err
				}
			}
		}
		// A short page is not exhaustion; only a successful empty page is.
		if len(rows) == 0 {
			if err := saveAptosDepositCursorWithContext(ctx, owner, end); err != nil {
				return err
			}
			if work != nil {
				return clearAptosReviewWork(ctx, owner)
			}
			return nil
		}
		for first := 0; first < len(rows); {
			pos, _ := rows[first].Position() // Complete page was validated first.
			last := first + 1
			for last < len(rows) {
				next, _ := rows[last].Position()
				if next.Version != pos.Version {
					break
				}
				last++
			}
			endOfGroup, _ := rows[last-1].Position()
			if endOfGroup.After(cursor) {
				if err := ctx.Err(); err != nil {
					return err
				}
				pending := make([]service.AptosIndexedDeposit, 0, last-first)
				for _, row := range rows[first:last] {
					position, _ := row.Position()
					if position.After(cursor) && configuredAptosDeposit(row, tokens) {
						pending = append(pending, row)
					}
				}
				if len(pending) > 0 {
					if err := verifyAptosIndexedTransaction(provider, owner, pos.Version, pending, tokens); err != nil {
						return err
					}
				}
				// Commit only the last verified event on this page, not the entire
				// version: another page can contain more events from the same tx.
				if err := saveAptosDepositCursorWithContext(ctx, owner, endOfGroup); err != nil {
					return err
				}
				cursor = endOfGroup
			}
			first = last
		}
		if work != nil {
			work.PageReady, work.Rows = false, nil
			if err := saveAptosReviewWork(ctx, owner, work); err != nil {
				return err
			}
		}
	}
	return nil // The next round resumes the exact event, including within a tx.
}

func validateAptosDepositPage(rows []service.AptosIndexedDeposit, owner string, after service.AptosEventPosition, head int64) error {
	return service.ValidateAptosDepositPage(rows, owner, after, head, aptosDepositPageSize)
}

func configuredAptosDeposit(row service.AptosIndexedDeposit, tokens []mdb.ChainToken) bool {
	amount, ok := new(big.Int).SetString(row.Amount.String(), 10)
	if !ok || amount.Sign() <= 0 {
		return false
	}
	for _, token := range tokens {
		if addressutil.MoveAssetMatches(row.Asset, token.ContractAddress) {
			return true
		}
	}
	return false
}

func verifyAptosIndexedTransaction(provider aptosDepositProvider, owner string, version int64, rows []service.AptosIndexedDeposit, tokens []mdb.ChainToken) error {
	selected := make([]service.AptosIndexedDeposit, 0)
	for _, row := range rows {
		pos, err := row.Position()
		if err != nil {
			return err
		}
		if pos.Version == version && configuredAptosDeposit(row, tokens) {
			selected = append(selected, row)
		}
	}
	var transfers []service.MoveObservedTransfer
	_, err := provider.Transaction(version, func(body []byte) error {
		var validationErr error
		transfers, validationErr = validateAptosIndexedTransaction(body, owner, version, selected, tokens)
		return validationErr
	})
	if err != nil {
		return err
	}
	// A validation attempt may be retried on another endpoint. Only settle after
	// the provider has accepted the complete response.
	for _, transfer := range transfers {
		if err := processAptosObservedTransferFunc(transfer); err != nil {
			return err
		}
	}
	return nil
}

func validateAptosIndexedTransaction(body []byte, owner string, version int64, rows []service.AptosIndexedDeposit, tokens []mdb.ChainToken) ([]service.MoveObservedTransfer, error) {
	tx := gjson.ParseBytes(body)
	gotVersion, parseErr := strconv.ParseInt(tx.Get("version").String(), 10, 64)
	if !gjson.ValidBytes(body) || !tx.IsObject() || parseErr != nil || gotVersion != version || tx.Get("type").String() != "user_transaction" || tx.Get("success").Type != gjson.True {
		return nil, fmt.Errorf("RPC did not return the indexed successful transaction version=%d", version)
	}
	wrapped := append(append([]byte{'['}, body...), ']')
	if err := validateAptosLedgerContents(wrapped); err != nil {
		return nil, err
	}
	return service.ParseAptosIndexedDeposits(body, owner, rows, tokens)
}
