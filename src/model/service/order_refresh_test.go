package service

import (
	"errors"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/xssnick/tonutils-go/address"
)

func TestSwitchNetworkLostParentReservationRollsBackChild(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	if err := data.SetSetting("rate", "rate.forced_rate_list", `{"cny":{"usdt":0.1,"ton":0.5}}`, "json"); err != nil {
		t.Fatal(err)
	}
	if _, err := data.AddWalletAddress("TTestTronAddress001"); err != nil {
		t.Fatal(err)
	}
	addr := address.MustParseAddr("EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I")
	if _, err := data.AddWalletAddressWithNetwork(mdb.NetworkTon, addr.StringRaw()); err != nil {
		t.Fatal(err)
	}
	parent, err := CreateTransaction(newCreateTransactionRequest(t.Name(), 10), nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := data.GetOrderInfoByTradeId(parent.TradeId)
	if err != nil {
		t.Fatal(err)
	}
	if err := data.UnLockTransactionByTradeId(parent.TradeId); err != nil {
		t.Fatal(err)
	}
	if _, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Token: "TON", Network: mdb.NetworkTon}); !errors.Is(err, constant.OrderStatusConflict) {
		t.Fatalf("lost parent reservation allowed switching: %v", err)
	}
	if count, err := data.CountSubOrders(parent.TradeId); err != nil || count != 0 {
		t.Fatalf("failed switch left a child order: %d %v", count, err)
	}
	var locks int64
	if err := dao.RuntimeDB.Model(&mdb.TransactionLock{}).Count(&locks).Error; err != nil || locks != 0 {
		t.Fatalf("failed switch leaked child reservation: %d %v", locks, err)
	}
	after, err := data.GetOrderInfoByTradeId(parent.TradeId)
	if err != nil || !data.PaymentDeadline(before).Equal(data.PaymentDeadline(after)) {
		t.Fatalf("failed switch changed parent deadline: %+v %v", after, err)
	}
}
