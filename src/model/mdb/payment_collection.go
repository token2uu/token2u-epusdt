package mdb

import "time"

const CollectionExclusive = "exclusive_actual"

// PaymentAddress is a reusable slot. TradeID is an ownership token: a delayed
// release must never clear a slot that has already been assigned to another order.
type PaymentAddress struct {
	ID             uint64    `gorm:"primaryKey" json:"id"`
	Network        string    `gorm:"size:32;uniqueIndex:payment_address_key,priority:1" json:"network"`
	Address        string    `gorm:"size:256;uniqueIndex:payment_address_key,priority:2" json:"address"`
	Token          string    `gorm:"size:32" json:"token"`
	AssetID        string    `gorm:"size:256;uniqueIndex:payment_address_key,priority:3" json:"asset_id"`
	TradeID        string    `gorm:"size:128;default:'';index" json:"trade_id"`
	LastReleasedAt int64     `gorm:"default:0" json:"last_released_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// PaymentReceipt is both the durable deduplication record and the exception
// inbox. Resolving an exception is an audit operation, never a payment callback.
type PaymentReceipt struct {
	ID            uint64     `gorm:"primaryKey" json:"id"`
	EventKey      string     `gorm:"size:64;uniqueIndex" json:"event_key"`
	Network       string     `gorm:"size:32;index" json:"network"`
	Address       string     `gorm:"size:256;index" json:"address"`
	Token         string     `gorm:"size:32" json:"token"`
	AssetID       string     `gorm:"size:256" json:"asset_id"`
	TxHash        string     `gorm:"size:256;index" json:"tx_hash"`
	EventID       string     `gorm:"size:256" json:"event_id"`
	RawAmount     string     `gorm:"size:120" json:"raw_amount"`
	Decimals      int        `json:"decimals"`
	Amount        string     `gorm:"size:120" json:"amount"`
	BlockTimeMs   int64      `json:"block_time_ms"`
	TradeID       string     `gorm:"size:128;index" json:"trade_id"`
	ParentTradeID string     `gorm:"size:128" json:"parent_trade_id"`
	Status        string     `gorm:"size:24;index" json:"status"`
	Reason        string     `gorm:"size:64" json:"reason"`
	Note          string     `gorm:"type:text" json:"note"`
	ResolvedBy    uint64     `json:"resolved_by"`
	ResolvedAt    *time.Time `json:"resolved_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
}

type PaymentScanCursor struct {
	Key string `gorm:"size:256;primaryKey"`
	// Includes resumable scan checkpoints as well as short cursor positions.
	// Unbounded strings map to TEXT (SQLite/Postgres) or LONGTEXT (MySQL).
	Position  string
	UpdatedAt time.Time
}
