package admin

import (
	_ "embed"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/labstack/echo/v4"
)

//go:embed collection_console.html
var collectionConsole string

//go:embed collection_tools.js
var collectionTools string

// These contain no account data. All data APIs below require the admin JWT.
func (c *BaseAdminController) CollectionConsole(ctx echo.Context) error {
	return ctx.HTML(http.StatusOK, collectionConsole)
}
func (c *BaseAdminController) CollectionTools(ctx echo.Context) error {
	return ctx.Blob(http.StatusOK, "application/javascript; charset=utf-8", []byte(collectionTools))
}

func (c *BaseAdminController) ListPaymentReceipts(ctx echo.Context) error {
	page, _ := strconv.Atoi(ctx.QueryParam("page"))
	if page < 1 {
		page = 1
	}
	size := 25
	q := dao.Mdb.Model(&mdb.PaymentReceipt{})
	if status := strings.TrimSpace(ctx.QueryParam("status")); status != "" {
		q = q.Where("status = ?", status)
	}
	if network := strings.TrimSpace(ctx.QueryParam("network")); network != "" {
		q = q.Where("network = ?", network)
	}
	if key := strings.TrimSpace(ctx.QueryParam("keyword")); key != "" {
		q = q.Where("trade_id = ? OR parent_trade_id = ? OR tx_hash = ? OR address = ?", key, key, key, key)
	}
	var count int64
	if err := q.Count(&count).Error; err != nil {
		return c.FailJson(ctx, err)
	}
	var rows []mdb.PaymentReceipt
	if err := q.Order("id DESC").Limit(size).Offset((page - 1) * size).Find(&rows).Error; err != nil {
		return c.FailJson(ctx, err)
	}
	return c.SucJson(ctx, map[string]interface{}{"list": rows, "total": count, "page": page, "page_size": size})
}
func (c *BaseAdminController) ListPaymentAddresses(ctx echo.Context) error {
	var rows []mdb.PaymentAddress
	q := dao.Mdb.Order("network ASC, token ASC, last_released_at ASC")
	if network := strings.TrimSpace(ctx.QueryParam("network")); network != "" {
		q = q.Where("network = ?", network)
	}
	// Intended for the single-instance address pool. Bounded response size.
	if err := q.Limit(2000).Find(&rows).Error; err != nil {
		return c.FailJson(ctx, err)
	}
	return c.SucJson(ctx, rows)
}
func (c *BaseAdminController) ResolvePaymentReceipt(ctx echo.Context) error {
	id, err := strconv.ParseUint(ctx.Param("id"), 10, 64)
	if err != nil || id == 0 {
		return c.FailJson(ctx, constant.ParamsMarshalErr)
	}
	var body struct {
		Note string `json:"note"`
	}
	if err := ctx.Bind(&body); err != nil {
		return c.FailJson(ctx, constant.ParamsMarshalErr)
	}
	body.Note = strings.TrimSpace(body.Note)
	actor := currentAdminUserID(ctx)
	if actor == 0 {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}
	if body.Note == "" || len([]rune(body.Note)) > 2000 {
		return echo.NewHTTPError(http.StatusBadRequest, "请填写 1–2000 字的处理说明")
	}
	result := dao.Mdb.Model(&mdb.PaymentReceipt{}).Where("id = ? AND status = ?", id, "exception").Updates(map[string]interface{}{
		"status": "resolved", "note": body.Note, "resolved_by": actor, "resolved_at": time.Now(),
	})
	if result.Error != nil {
		return c.FailJson(ctx, result.Error)
	}
	if result.RowsAffected != 1 {
		return echo.NewHTTPError(http.StatusConflict, "记录不存在或已处理，请刷新列表")
	}
	// No order mutation or success callback: finance handles refunds/adjustments
	// independently and this immutable resolution note provides the audit trail.
	return c.SucJson(ctx, map[string]interface{}{"id": id, "status": "resolved"})
}
