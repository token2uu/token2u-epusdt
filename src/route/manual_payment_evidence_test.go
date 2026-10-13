package route

import (
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/shopspring/decimal"
	"testing"
)

// Route tests replace only RPC. They still exercise historical ownership and
// settlement using an exact transfer inside the fixture's assignment window.
func manualRouteEvidence(t *testing.T, order *mdb.Orders) request.ManualPaymentEvidence {
	t.Helper()
	token, err := data.GetEnabledChainTokenBySymbol(order.Network, order.Token)
	if err != nil || token == nil || token.ID == 0 {
		t.Fatalf("missing fixture asset: %+v %v", token, err)
	}
	start, end := data.PaymentAssignmentTimeMs(order), data.PaymentDeadline(order).UnixMilli()
	if end <= start+2000 {
		t.Fatal("fixture has no payable interval")
	}
	return request.ManualPaymentEvidence{RawAmount: decimal.NewFromFloat(order.ActualAmount).Shift(int32(token.Decimals)).StringFixed(0), Decimals: token.Decimals, BlockTimeMs: (start + end) / 2, AssetID: token.ContractAddress}
}
