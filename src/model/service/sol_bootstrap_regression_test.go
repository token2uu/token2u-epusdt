package service

import (
	"strconv"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/spf13/viper"
)

func TestSolBootstrapCoversPendingOrderAndFreezesAllAccounts(t *testing.T) {
	for _, outage := range []bool{false, true} {
		name := "first_scan"
		if outage {
			name = "first_query_outage"
		}
		t.Run(name, func(t *testing.T) {
			l := setupSolCursorLedger(t)
			isolateSolThrottle(t, 1000)
			isolateSolHistoryBackoff(t)
			data.ResetRpcEndpointPolicyForTest()
			t.Cleanup(data.ResetRpcEndpointPolicyForTest)
			data.ResetRpcFailoverForTest()
			t.Cleanup(data.ResetRpcFailoverForTest)
			viper.Set("order_expiration_time", 10)
			order := solCursorOrder(t)
			created := time.Now().Add(-40 * time.Minute)
			if err := dao.Mdb.Model(order).Updates(map[string]interface{}{
				"created_at": created, "assigned_at_ms": created.UnixMilli(), "payment_expires_at": time.Now().Add(10 * time.Minute),
			}).Error; err != nil {
				t.Fatal(err)
			}
			l.add("B", 2, "3000000")
			stamp := time.Now().Add(-20 * time.Minute).Unix()
			l.signatures[0].BlockTime = &stamp
			l.transactions["B"].(map[string]interface{})["blockTime"] = stamp
			starts := make(map[string]string)
			if outage {
				l.rateLimited = true
				if err := ScanSolanaAddress(solCursorTestOwner); err == nil {
					t.Fatal("outage accepted")
				}
				for _, address := range []string{solCursorTestOwner, l.ata} {
					key := "solana:signatures:" + address + ":start_time"
					value, err := data.LoadPaymentCursor(key)
					seconds, parseErr := strconv.ParseInt(value, 10, 64)
					if err != nil || parseErr != nil || seconds > created.Unix() {
						t.Fatalf("initial boundary not frozen before request: address=%s value=%q err=%v", address, value, err)
					}
					starts[key] = value
				}
				assertSolCursor(t, l, "")
				var count int64
				dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count)
				if count != 0 {
					t.Fatal("outage consumed payment")
				}
				l.rateLimited = false
				isolateSolThrottle(t, 1000)
				data.ResetRpcEndpointPolicyForTest()
				data.ResetRpcFailoverForTest()
				viper.Set("order_expiration_time", 1)
			}
			if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
				t.Fatal(err)
			}
			assertSolCursor(t, l, "B")
			assertSolCursorSettlement(t, order)
			for key, want := range starts {
				got, err := data.LoadPaymentCursor(key)
				if err != nil || got != want {
					t.Fatalf("recovery changed start: got=%s want=%s err=%v", got, want, err)
				}
			}
			clearProcessedSignatures() // A restarted process relies on durable progress.
			if err := ScanSolanaAddress(solCursorTestOwner); err != nil {
				t.Fatal(err)
			}
			assertSolCursorSettlement(t, order)
		})
	}
}
