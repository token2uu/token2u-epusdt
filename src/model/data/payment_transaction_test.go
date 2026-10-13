package data

import (
	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"strings"
	"testing"
)

func TestPaymentTransactionLookupScopesHexAliasesToNetwork(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	body := strings.Repeat("aB", 32)
	for _, network := range []string{mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma, mdb.NetworkTron, mdb.NetworkAptos} {
		order := &mdb.Orders{TradeId: network, OrderId: network, Network: network, Status: mdb.StatusPaySuccess, BlockTransactionId: "0X" + body}
		if err := dao.Mdb.Create(order).Error; err != nil {
			t.Fatal(err)
		}
		for _, alias := range []string{body, "0x" + strings.ToLower(body), "0X" + strings.ToUpper(body)} {
			got, err := GetOrderByPaymentTransactionWithTransaction(dao.Mdb, network, alias)
			if err != nil || got.ID != order.ID {
				t.Fatalf("%s lookup matched wrong order: %+v %v", network, got, err)
			}
		}
	}
}

func TestPaymentTransactionLookupKeepsSolanaCaseSensitive(t *testing.T) {
	t.Cleanup(testutil.SetupTestDatabases(t))
	sig := strings.Repeat("aB", 32)
	order := &mdb.Orders{TradeId: "sol", OrderId: "sol", Network: mdb.NetworkSolana, BlockTransactionId: sig, Status: mdb.StatusPaySuccess}
	if err := dao.Mdb.Create(order).Error; err != nil {
		t.Fatal(err)
	}
	got, err := GetOrderByPaymentTransactionWithTransaction(dao.Mdb, mdb.NetworkSolana, strings.ToLower(sig))
	if err != nil || got.ID != 0 {
		t.Fatalf("distinct signature collapsed: %+v %v", got, err)
	}
	got, err = GetOrderByPaymentTransactionWithTransaction(dao.Mdb, mdb.NetworkSolana, sig)
	if err != nil || got.ID != order.ID {
		t.Fatalf("original signature not found: %+v %v", got, err)
	}
}
