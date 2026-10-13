package task

import (
	"fmt"
	"time"
)

// Find the last block strictly before the frozen payment boundary. Probe
// backwards exponentially before binary search so recent-history nodes need
// not serve arbitrary blocks halfway back to genesis. Errors never yield a
// usable cursor. Block zero is the predecessor of the first scannable block.
func paymentScanPredecessor(head int64, start time.Time, timestamp func(int64) (int64, error)) (int64, error) {
	if head < 0 || start.UnixMilli() <= 0 {
		return 0, fmt.Errorf("invalid bootstrap boundary")
	}
	if head == 0 {
		return 0, nil
	}
	read := func(n int64) (int64, error) {
		ts, err := timestamp(n)
		if err != nil {
			return 0, err
		}
		if ts <= 0 {
			return 0, fmt.Errorf("missing bootstrap block time at %d", n)
		}
		return ts, nil
	}
	cutoff := start.UnixMilli()
	high := head
	ts, err := read(high)
	if err != nil {
		return 0, err
	}
	if ts < cutoff {
		return high, nil
	}
	low := int64(0)
	for step := int64(1); ; {
		probe := head - step
		if probe < 1 {
			probe = 1
		}
		ts, err = read(probe)
		if err != nil {
			return 0, err
		}
		if ts < cutoff {
			low = probe
			break
		}
		high = probe
		if probe == 1 {
			return 0, nil
		}
		if step > head/2 {
			step = head - 1
		} else {
			step *= 2
		}
	}
	for high-low > 1 {
		mid := low + (high-low)/2
		ts, err = read(mid)
		if err != nil {
			return 0, err
		}
		if ts < cutoff {
			low = mid
		} else {
			high = mid
		}
	}
	return low, nil
}
