package service

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
)

// ERC20 Transfer has two indexed addresses and one ABI-encoded uint256.
// Never interpret truncated data as a smaller payment or an empty zero payment.
func ValidateEvmTransferLog(event types.Log) error {
	if len(event.Topics) != 3 || event.Topics[0] != erc20TransferEventHash || len(event.Data) != 32 {
		return fmt.Errorf("invalid ERC20 Transfer topics or amount encoding")
	}
	for _, topic := range event.Topics[1:] {
		for _, b := range topic[:12] {
			if b != 0 {
				return fmt.Errorf("invalid ERC20 Transfer address padding")
			}
		}
	}
	return nil
}
