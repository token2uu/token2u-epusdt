package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/GMWalletApp/epusdt/model/data"
)

var solReviewsInFlight sync.Map

type solSignatureQuery struct {
	Address, Key, Until string
	Cutoff              int64
	Before, Next        string
	Done                bool
	Seen                map[string]bool
}

// Keep the frozen range and its verified pages together. A crash before saving
// a page retries it; a crash after settlement retries through durable receipts.
// High-water cursors still advance only after the entire range is processed.
type solScanWork struct {
	Version    int
	Queries    []solSignatureQuery
	Signatures []solSignatureResult
	Processed  map[string]bool
}

func solReviewWorkKey(owner string) string { return "solana:payment_review_work:" + owner }

func (work *solScanWork) save(ctx context.Context, owner string) error {
	if !IsPaidPaymentReview(ctx) {
		return nil
	}
	encoded, err := json.Marshal(work)
	if err != nil {
		return err
	}
	return data.SavePaymentCursor(solReviewWorkKey(owner), string(encoded))
}

func collectSolanaSignatures(ctx context.Context, owner string, queries []solSignatureQuery) (*solScanWork, error) {
	work := &solScanWork{Version: 1, Queries: queries, Processed: make(map[string]bool)}
	if IsPaidPaymentReview(ctx) {
		stored, err := data.LoadPaymentCursor(solReviewWorkKey(owner))
		if err != nil {
			return nil, err
		}
		if stored != "" {
			work = &solScanWork{}
			if err := json.Unmarshal([]byte(stored), work); err != nil {
				return nil, fmt.Errorf("invalid Solana review checkpoint: %w", err)
			}
			if work.Version != 1 || len(work.Queries) == 0 || work.Processed == nil {
				return nil, fmt.Errorf("invalid Solana review checkpoint")
			}
			for _, query := range work.Queries {
				if query.Address == "" || query.Key != "solana:payment_review:"+query.Address || query.Cutoff <= 0 || (query.Before != "" && !query.Seen[query.Before]) {
					return nil, fmt.Errorf("invalid Solana review query checkpoint")
				}
			}
		}
	}
	seen := make(map[string]bool, len(work.Signatures))
	for _, sig := range work.Signatures {
		seen[sig.Signature] = true
	}
	// Initial boundaries for every account were frozen before the first RPC.
	for i := range work.Queries {
		query := &work.Queries[i]
		if query.Seen == nil {
			query.Seen = make(map[string]bool)
		}
		for !query.Done {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			body, err := solGetSignaturesForAddressPageWithContext(ctx, query.Address, 1000, query.Until, query.Before, query.Seen)
			if err != nil {
				return nil, err
			}
			batch, err := parseSolSignatureList(body, 1000)
			if err != nil {
				return nil, err
			}
			query.Done = len(batch) == 0
			if len(batch) > 0 {
				if query.Before == "" {
					query.Next = batch[0].Signature
				}
				for _, sig := range batch {
					query.Seen[sig.Signature] = true
					if query.Until == "" && sig.BlockTime != nil && *sig.BlockTime < query.Cutoff {
						query.Done = true
						continue
					}
					if !seen[sig.Signature] {
						seen[sig.Signature] = true
						work.Signatures = append(work.Signatures, sig)
					}
				}
				query.Before = batch[len(batch)-1].Signature
			}
			// A short page is not exhaustion: providers can impose a lower cap.
			if err := work.save(ctx, owner); err != nil {
				return nil, err
			}
		}
	}
	return work, nil
}
