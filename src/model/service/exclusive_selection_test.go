package service

import (
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/spf13/viper"
)

func exclusiveSelectionOrder(t *testing.T) *mdb.Orders {
	t.Helper()
	t.Cleanup(testutil.SetupTestDatabases(t))
	viper.Set("epay_exclusive_addresses", true)
	if _, err := data.AddWalletAddress("selection-tron"); err != nil {
		t.Fatal(err)
	}
	if _, err := data.AddWalletAddressWithNetwork(mdb.NetworkSolana, "selection-solana"); err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Create(&mdb.ChainToken{Network: mdb.NetworkSolana, Symbol: "USDT", ContractAddress: USDT_Mint, Decimals: 6, Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	req := newCreateTransactionRequest("selection-order", 5)
	req.PaymentType = mdb.PaymentTypeEpay
	resp, err := CreateTransaction(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	order, err := data.GetOrderInfoByTradeId(resp.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	return order
}

func assertExclusiveSelection(t *testing.T, before *mdb.Orders) {
	t.Helper()
	after, err := data.GetOrderInfoByTradeId(before.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if !after.IsSelected || !buildCheckoutResponse(after).IsSelected {
		t.Fatalf("selection lost after reload: %+v", after)
	}
	if after.CreatedAt.TimestampMilli() != before.CreatedAt.TimestampMilli() || !after.PaymentExpiresAt.Equal(*before.PaymentExpiresAt) || after.AssignedAtMs != before.AssignedAtMs || after.ReceiveAddress != before.ReceiveAddress || after.Status != before.Status {
		t.Fatalf("selection changed payment assignment or expiry: before=%+v after=%+v", before, after)
	}
}

func TestExclusiveSelectionConfirmsOriginalNetwork(t *testing.T) {
	parent := exclusiveSelectionOrder(t)
	for i := 0; i < 2; i++ {
		resp, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Token: "usdt", Network: "TRON"})
		if err != nil || !resp.IsSelected || resp.TradeId != parent.TradeId {
			t.Fatalf("confirmation failed: %+v %v", resp, err)
		}
		assertExclusiveSelection(t, parent)
	}
	if count, err := data.CountSubOrders(parent.TradeId); err != nil || count != 0 {
		t.Fatalf("same network created child: count=%d err=%v", count, err)
	}
}

func TestExclusiveSelectionCreatesAndReusesChild(t *testing.T) {
	parent := exclusiveSelectionOrder(t)
	req := &request.SwitchNetworkRequest{TradeId: parent.TradeId, Token: "USDT", Network: mdb.NetworkSolana}
	resp, err := SwitchNetwork(req)
	if err != nil || !resp.IsSelected || resp.TradeId == parent.TradeId {
		t.Fatalf("child selection failed: %+v %v", resp, err)
	}
	assertExclusiveSelection(t, parent)
	child, err := data.GetOrderInfoByTradeId(resp.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if !child.PaymentExpiresAt.Equal(*parent.PaymentExpiresAt) {
		t.Fatal("child did not inherit expiry")
	}
	// Simulate unselected records left by the previous implementation.
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("trade_id IN ?", []string{parent.TradeId, child.TradeId}).Update("is_selected", false).Error; err != nil {
		t.Fatal(err)
	}
	resp, err = SwitchNetwork(req)
	if err != nil || !resp.IsSelected || resp.TradeId != child.TradeId {
		t.Fatalf("child not reused/selected: %+v %v", resp, err)
	}
	assertExclusiveSelection(t, parent)
	assertExclusiveSelection(t, child)
	if count, err := data.CountSubOrders(parent.TradeId); err != nil || count != 1 {
		t.Fatalf("child duplicated: count=%d err=%v", count, err)
	}
}

func TestExclusiveSelectionFailureRollsBack(t *testing.T) {
	for _, tc := range []struct{ name, target, trigger string }{
		{"original_selection_write", mdb.NetworkTron, `CREATE TRIGGER reject_selection BEFORE UPDATE OF is_selected ON orders
WHEN NEW.is_selected = 1 BEGIN SELECT RAISE(ABORT, 'selection write failed'); END`},
		{"parent_selection_write", mdb.NetworkSolana, `CREATE TRIGGER reject_selection BEFORE UPDATE OF is_selected ON orders
WHEN NEW.is_selected = 1 BEGIN SELECT RAISE(ABORT, 'selection write failed'); END`},
		{"child_creation", mdb.NetworkSolana, `CREATE TRIGGER reject_child BEFORE INSERT ON orders
WHEN NEW.parent_trade_id <> '' BEGIN SELECT RAISE(ABORT, 'child creation failed'); END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := exclusiveSelectionOrder(t)
			if err := dao.Mdb.Exec(tc.trigger).Error; err != nil {
				t.Fatal(err)
			}
			resp, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Token: "USDT", Network: tc.target})
			if err == nil || resp != nil {
				t.Fatalf("failed write reported success: %+v %v", resp, err)
			}
			after, err := data.GetOrderInfoByTradeId(parent.TradeId)
			if err != nil || after.IsSelected {
				t.Fatalf("failed selection persisted: %+v %v", after, err)
			}
			if count, err := data.CountSubOrders(parent.TradeId); err != nil || count != 0 {
				t.Fatalf("child survived failed selection: count=%d err=%v", count, err)
			}
			var occupied int64
			if err := dao.Mdb.Model(&mdb.PaymentAddress{}).Where("trade_id <> ?", "").Count(&occupied).Error; err != nil || occupied != 1 {
				t.Fatalf("failed selection leaked address: count=%d err=%v", occupied, err)
			}
		})
	}
}

func TestExclusiveSelectionExistingChildWriteIsAtomic(t *testing.T) {
	parent := exclusiveSelectionOrder(t)
	req := &request.SwitchNetworkRequest{TradeId: parent.TradeId, Token: "USDT", Network: mdb.NetworkSolana}
	resp, err := SwitchNetwork(req)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{parent.TradeId, resp.TradeId}
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("trade_id IN ?", ids).Update("is_selected", false).Error; err != nil {
		t.Fatal(err)
	}
	if err := dao.Mdb.Exec(`CREATE TRIGGER reject_child_selection BEFORE UPDATE OF is_selected ON orders
WHEN NEW.parent_trade_id <> '' AND NEW.is_selected = 1 BEGIN SELECT RAISE(ABORT, 'child selection failed'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if resp, err := SwitchNetwork(req); err == nil || resp != nil {
		t.Fatalf("failed child write reported success: %+v %v", resp, err)
	}
	for _, id := range ids {
		order, err := data.GetOrderInfoByTradeId(id)
		if err != nil || order.IsSelected {
			t.Fatalf("partial selection committed: %+v %v", order, err)
		}
	}
}
