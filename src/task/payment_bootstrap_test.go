package task

import (
	"errors"
	"testing"
	"time"
)

func TestPaymentBootstrapFindsBoundaryWithoutSkippingEqualTimestamps(t *testing.T) {
	base := time.Unix(1700000000, 0)
	// Two blocks can have the same second timestamp. Both must be scanned.
	for _, tc := range []struct {
		name   string
		cutoff time.Time
		want   int64
	}{
		{"before-first", base, 0},
		{"equal-first", base.Add(time.Second), 0},
		{"middle", base.Add(101 * time.Second), 200},
		{"latest", base.Add(500 * time.Second), 998},
		{"past-head", base.Add(501 * time.Second), 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			got, err := paymentScanPredecessor(1000, tc.cutoff, func(n int64) (int64, error) {
				calls++
				return base.Add(time.Duration((n+1)/2) * time.Second).UnixMilli(), nil
			})
			if err != nil || got != tc.want {
				t.Fatalf("predecessor=%d want=%d err=%v", got, tc.want, err)
			}
			if calls > 25 {
				t.Fatalf("unexpected linear history search: %d requests", calls)
			}
		})
	}
}

func TestPaymentBootstrapRejectsUnavailableOrMissingBlockTime(t *testing.T) {
	for _, failure := range []string{"rpc", "timestamp"} {
		t.Run(failure, func(t *testing.T) {
			failureErr := errors.New("history unavailable")
			got, err := paymentScanPredecessor(100, time.Unix(100, 0), func(n int64) (int64, error) {
				if n == 99 {
					if failure == "rpc" {
						return 0, failureErr
					}
					return 0, nil
				}
				return 200000, nil
			})
			if err == nil || got != 0 {
				t.Fatalf("unsafe cursor on %s failure: %d %v", failure, got, err)
			}
		})
	}
}
