package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/middleware"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/labstack/echo/v4"
)

func TestResolveReceiptIsAuditedWithoutChangingOrder(t *testing.T) {
	defer testutil.SetupTestDatabases(t)()
	order := mdb.Orders{TradeId: "closed", OrderId: "closed", Status: mdb.StatusExpired, CallBackConfirm: mdb.CallBackConfirmOk}
	if err := dao.Mdb.Create(&order).Error; err != nil {
		t.Fatal(err)
	}
	receipt := mdb.PaymentReceipt{EventKey: "event", TradeID: order.TradeId, Status: "exception", Reason: "late_payment"}
	if err := dao.Mdb.Create(&receipt).Error; err != nil {
		t.Fatal(err)
	}
	call := func(note string) error {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(note))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		ctx := echo.New().NewContext(req, httptest.NewRecorder())
		ctx.SetParamNames("id")
		ctx.SetParamValues("1")
		ctx.Set(middleware.AdminUserIDKey, uint64(7))
		return Ctrl.ResolvePaymentReceipt(ctx)
	}
	if err := call(`{"note":"已核对并线下退款，凭证 R-1"}`); err != nil {
		t.Fatal(err)
	}
	if err := call(`{"note":"overwrite"}`); err == nil {
		t.Fatal("audit note overwritten")
	}
	dao.Mdb.First(&receipt, receipt.ID)
	dao.Mdb.First(&order, order.ID)
	if receipt.Status != "resolved" || receipt.ResolvedBy != 7 || receipt.ResolvedAt == nil || receipt.Note == "overwrite" {
		t.Fatalf("bad audit: %+v", receipt)
	}
	if order.Status != mdb.StatusExpired || order.CallBackConfirm != mdb.CallBackConfirmOk {
		t.Fatal("resolution queued callback or changed order")
	}
}
