package data

import (
	"math/big"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/libtnb/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func TestPaymentProcessRestart(t *testing.T) {
	for _, stage := range []string{"before-commit", "after-commit"} {
		t.Run(stage, func(t *testing.T) {
			t.Cleanup(testutil.SetupTestDatabases(t))
			deadline := time.Now().Add(time.Minute)
			order := &mdb.Orders{TradeId: "process-restart", OrderId: "process-restart", Network: mdb.NetworkTron, Token: "USDT", Amount: 5, ActualAmount: 5, Status: mdb.StatusWaitPay, PaymentExpiresAt: &deadline}
			asset, err := GetEnabledChainTokenBySymbol(order.Network, order.Token)
			if err != nil {
				t.Fatal(err)
			}
			if err := CreateExclusivePayment(order, *asset, []mdb.WalletAddress{{Address: "restart-wallet"}}, false); err != nil {
				t.Fatal(err)
			}
			if err := dao.Mdb.Model(order).Update("assigned_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Error; err != nil {
				t.Fatal(err)
			}
			var databases []struct {
				Name string
				File string
			}
			if err := dao.Mdb.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
				t.Fatal(err)
			}
			dbPath := ""
			for _, db := range databases {
				if db.Name == "main" {
					dbPath = db.File
				}
			}
			if dbPath == "" {
				t.Fatal("missing persistent DB path")
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestPaymentProcessCrashHelper$")
			cmd.Env = append(os.Environ(), "EPUSDT_CRASH_DB="+dbPath, "EPUSDT_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 17 {
				t.Fatalf("crash hook not reached: %v %s", err, out)
			}
			var got mdb.Orders
			if err := dao.Mdb.Where("trade_id = ?", order.TradeId).First(&got).Error; err != nil {
				t.Fatal(err)
			}
			want := mdb.StatusWaitPay
			if stage == "after-commit" {
				want = mdb.StatusPaySuccess
			}
			if got.Status != want {
				t.Fatalf("partial settlement survived crash: %+v", got)
			}
			var receipts int64
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil {
				t.Fatal(err)
			}
			wantReceipts := int64(0)
			if stage == "after-commit" {
				wantReceipts = 1
			}
			if receipts != wantReceipts {
				t.Fatalf("receipt state after crash: %d", receipts)
			}
			p := ObservedPayment{Network: order.Network, Address: "restart-wallet", Token: "USDT", AssetID: asset.ContractAddress, TxHash: "restart-tx", EventID: "contract:0", RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: time.Now().Add(-10 * time.Second).UnixMilli()}
			for i := 0; i < 2; i++ {
				if handled, err := ProcessExclusivePayment(p); err != nil || !handled {
					t.Fatalf("restart replay handled=%v err=%v", handled, err)
				}
			}
			if err := dao.Mdb.Where("trade_id = ?", order.TradeId).First(&got).Error; err != nil {
				t.Fatal(err)
			}
			if got.Status != mdb.StatusPaySuccess || got.CallBackConfirm != mdb.CallBackConfirmNo {
				t.Fatalf("recovery did not enqueue success: %+v", got)
			}
			if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&receipts).Error; err != nil || receipts != 1 {
				t.Fatalf("duplicate receipts after restart: %d %v", receipts, err)
			}
			var slot mdb.PaymentAddress
			if err := dao.Mdb.First(&slot).Error; err != nil || slot.TradeID != "" {
				t.Fatalf("slot not released: %+v %v", slot, err)
			}
		})
	}
}

func TestPaymentProcessCrashHelper(t *testing.T) {
	path := os.Getenv("EPUSDT_CRASH_DB")
	if path == "" {
		t.Skip("subprocess only")
	}
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{NamingStrategy: schema.NamingStrategy{SingularTable: true}, Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	dao.Mdb = db
	if os.Getenv("EPUSDT_CRASH_STAGE") == "before-commit" {
		if err := db.Callback().Create().After("gorm:create").Register("test:crash", func(tx *gorm.DB) {
			if tx.Statement.Table == "payment_receipt" && tx.Error == nil {
				os.Exit(17)
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	asset, err := GetEnabledChainTokenBySymbol(mdb.NetworkTron, "USDT")
	if err != nil {
		t.Fatal(err)
	}
	p := ObservedPayment{Network: mdb.NetworkTron, Address: "restart-wallet", Token: "USDT", AssetID: asset.ContractAddress, TxHash: "restart-tx", EventID: "contract:0", RawAmount: big.NewInt(5_000_000), Decimals: 6, BlockTimeMs: time.Now().Add(-10 * time.Second).UnixMilli()}
	if handled, err := ProcessExclusivePayment(p); err != nil || !handled {
		t.Fatalf("settle handled=%v err=%v", handled, err)
	}
	os.Exit(17)
}
