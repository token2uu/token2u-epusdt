package service

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	"github.com/GMWalletApp/epusdt/util/http_client"
	applog "github.com/GMWalletApp/epusdt/util/log"
	"github.com/go-resty/resty/v2"
	"github.com/libtnb/sqlite"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func TestFlowReviewOkPayProcessRestart(t *testing.T) {
	parent := reviewOkPaySetup(t)
	var databases []struct {
		Name string
		File string
	}
	if err := dao.Mdb.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		t.Fatal(err)
	}
	path := ""
	for _, db := range databases {
		if db.Name == "main" {
			path = db.File
		}
	}
	if path == "" {
		t.Fatal("missing persistent DB")
	}
	payloadPath := filepath.Join(t.TempDir(), "request.json")
	cmd := exec.Command(os.Args[0], "-test.run=^TestOkPayProcessCrashHelper$")
	cmd.Env = append(os.Environ(), "EPUSDT_OKPAY_DB="+path, "EPUSDT_OKPAY_PARENT="+parent.TradeId, "EPUSDT_OKPAY_REQUEST="+payloadPath)
	out, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 17 {
		t.Fatalf("create crash hook not reached: %v %s", err, out)
	}
	encoded, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatal(err)
	}
	var initial url.Values
	if err := json.Unmarshal(encoded, &initial); err != nil {
		t.Fatal(err)
	}
	childID := initial.Get("unique_id")
	before, err := data.GetProviderOrderByTradeIDAndProvider(childID, mdb.PaymentProviderOkPay)
	if err != nil || before.Status != mdb.ProviderOrderStatusCreating || before.PayURL != "" {
		t.Fatalf("wrong interrupted state: %+v %v", before, err)
	}
	if err := data.SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeString); err != nil {
		t.Fatal(err)
	}
	var resumed url.Values
	http_client.ClientFactory = func() *resty.Client {
		return resty.New().SetTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			resumed = r.PostForm
			return reviewOkPayResponse(), nil
		}))
	}
	view, err := SwitchNetwork(&request.SwitchNetworkRequest{TradeId: parent.TradeId, Network: "okpay", Token: "USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if view.TradeId != childID || view.PaymentUrl == "" || !reflect.DeepEqual(initial, resumed) {
		t.Fatalf("restart created a different attempt: initial=%v resumed=%v view=%+v", initial, resumed, view)
	}
	var count int64
	if err := dao.Mdb.Model(&mdb.Orders{}).Where("parent_trade_id = ?", parent.TradeId).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("restart duplicated child: %d %v", count, err)
	}
}

func TestOkPayProcessCrashHelper(t *testing.T) {
	path := os.Getenv("EPUSDT_OKPAY_DB")
	if path == "" {
		t.Skip("subprocess only")
	}
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{NamingStrategy: schema.NamingStrategy{SingularTable: true}, Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	dao.Mdb = db
	applog.Sugar = zap.NewNop().Sugar()
	viper.Set("epay_exclusive_addresses", true)
	viper.Set("order_expiration_time", 10)
	viper.Set("app_uri", "https://example.com")
	config.SettingsGetString = func(key string) string { return data.GetSettingString(key, "") }
	http_client.ClientFactory = func() *resty.Client {
		return resty.New().SetTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(r.PostForm)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(os.Getenv("EPUSDT_OKPAY_REQUEST"), encoded, 0600); err != nil {
				t.Fatal(err)
			}
			os.Exit(17)
			return nil, nil
		}))
	}
	_, err = SwitchNetwork(&request.SwitchNetworkRequest{TradeId: os.Getenv("EPUSDT_OKPAY_PARENT"), Network: "okpay", Token: "USDT"})
	t.Fatalf("did not reach provider request: %v", err)
}
