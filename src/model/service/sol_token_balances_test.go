package service

import (
	"testing"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

// Turn the mock payment into an unchecked SPL transfer. Its asset identity
// must come from transaction balances rather than the instruction itself.
func solUncheckedPayment(l *solCursorLedger) (map[string]interface{}, map[string]interface{}, map[string]interface{}) {
	l.add("B", 2, "3000000")
	detail := l.transactions["B"].(map[string]interface{})
	message := detail["transaction"].(map[string]interface{})["message"].(map[string]interface{})
	message["accountKeys"] = []interface{}{map[string]interface{}{"pubkey": "source-ata"}, map[string]interface{}{"pubkey": l.ata}}
	parsed := message["instructions"].([]interface{})[0].(map[string]interface{})["parsed"].(map[string]interface{})
	parsed["type"] = "transfer"
	parsed["info"] = map[string]interface{}{"source": "source-ata", "destination": l.ata, "amount": "3000000"}
	row := map[string]interface{}{"accountIndex": 1, "mint": USDT_Mint,
		"uiTokenAmount": map[string]interface{}{"amount": "3000000", "decimals": 6}}
	return message, detail["meta"].(map[string]interface{}), row
}

func TestSolCursorUncheckedBalancesRetryAfterRepair(t *testing.T) {
	for _, balanceField := range []string{"postTokenBalances", "preTokenBalances"} {
		for _, corruption := range []string{"missing_decimals", "null_decimals", "string_decimals", "negative_decimals", "fractional_decimals", "overflow_decimals", "missing_index", "null_index", "string_index", "negative_index", "fractional_index", "out_of_range_index", "missing_mint", "empty_mint", "number_mint", "invalid_mint", "invalid_array", "missing_account_keys", "conflicting_mint", "conflicting_decimals"} {
			t.Run(balanceField+"/"+corruption, func(t *testing.T) {
				l := setupSolCursorLedger(t)
				o := solCursorOrder(t)
				if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
					t.Fatal(err)
				}
				message, meta, row := solUncheckedPayment(l)
				meta[balanceField] = []interface{}{row}
				keys := message["accountKeys"]
				switch corruption {
				case "missing_decimals":
					delete(row["uiTokenAmount"].(map[string]interface{}), "decimals")
				case "null_decimals":
					row["uiTokenAmount"].(map[string]interface{})["decimals"] = nil
				case "string_decimals":
					row["uiTokenAmount"].(map[string]interface{})["decimals"] = "6"
				case "negative_decimals":
					row["uiTokenAmount"].(map[string]interface{})["decimals"] = -1
				case "fractional_decimals":
					row["uiTokenAmount"].(map[string]interface{})["decimals"] = 6.5
				case "overflow_decimals":
					row["uiTokenAmount"].(map[string]interface{})["decimals"] = 256
				case "missing_index":
					delete(row, "accountIndex")
				case "null_index":
					row["accountIndex"] = nil
				case "string_index":
					row["accountIndex"] = "1"
				case "negative_index":
					row["accountIndex"] = -1
				case "fractional_index":
					row["accountIndex"] = 1.5
				case "out_of_range_index":
					row["accountIndex"] = 2
				case "missing_mint":
					delete(row, "mint")
				case "empty_mint":
					row["mint"] = ""
				case "number_mint":
					row["mint"] = 123
				case "invalid_mint":
					row["mint"] = "not-a-mint"
				case "invalid_array":
					meta[balanceField] = map[string]interface{}{}
				case "missing_account_keys":
					delete(message, "accountKeys")
				case "conflicting_mint", "conflicting_decimals":
					other := "preTokenBalances"
					if balanceField == other {
						other = "postTokenBalances"
					}
					mint, decimals := USDC_Mint, 6
					if corruption == "conflicting_decimals" {
						mint, decimals = USDT_Mint, 9
					}
					meta[other] = []interface{}{map[string]interface{}{"accountIndex": 0, "mint": mint, "uiTokenAmount": map[string]interface{}{"decimals": decimals}}}
				}
				scanSolCursor()
				assertSolCursor(t, l, "A")
				got, err := data.GetOrderInfoByTradeId(o.TradeId)
				if err != nil || got.Status != mdb.StatusWaitPay || got.PaidAmount != 0 {
					t.Fatalf("incomplete metadata settled payment: %+v %v", got, err)
				}
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
					t.Fatalf("incomplete metadata consumed receipt: %d %v", count, err)
				}
				if _, cached := gProcessedSignatures.Load(solCursorTestOwner + ":B"); cached {
					t.Fatal("incomplete metadata cached")
				}
				// Restore the same transaction, without resetting its persisted cursor.
				delete(meta, "preTokenBalances")
				delete(meta, "postTokenBalances")
				meta[balanceField] = []interface{}{map[string]interface{}{"accountIndex": 1, "mint": USDT_Mint, "uiTokenAmount": map[string]interface{}{"amount": "3000000", "decimals": 6}}}
				message["accountKeys"] = keys
				clearProcessedSignatures()
				scanSolCursor()
				assertSolCursor(t, l, "B")
				assertSolCursorSettlement(t, o)
				scanSolCursor()
				assertSolCursorSettlement(t, o)
			})
		}
	}
}

func TestSolCursorClosedTokenAccountsDoNotBlockPayment(t *testing.T) {
	for _, inner := range []bool{false, true} {
		name := "outer"
		if inner {
			name = "inner"
		}
		t.Run(name, func(t *testing.T) {
			l := setupSolCursorLedger(t)
			o := solCursorOrder(t)
			l.add("B", 2, "3000000")
			detail := l.transactions["B"].(map[string]interface{})
			message := detail["transaction"].(map[string]interface{})["message"].(map[string]interface{})
			message["accountKeys"] = []interface{}{map[string]interface{}{"pubkey": "source-ata"}, map[string]interface{}{"pubkey": l.ata}, map[string]interface{}{"pubkey": "temp-source"}, map[string]interface{}{"pubkey": "temp-dest"}}
			extra := []interface{}{
				map[string]interface{}{"programId": TokenProgramID, "parsed": map[string]interface{}{"type": "transfer", "info": map[string]interface{}{"source": "temp-source", "destination": "temp-dest", "amount": "1000000000"}}},
				map[string]interface{}{"programId": TokenProgramID, "parsed": map[string]interface{}{"type": "closeAccount", "info": map[string]interface{}{"account": "temp-source", "destination": "owner", "owner": "owner"}}},
				map[string]interface{}{"programId": TokenProgramID, "parsed": map[string]interface{}{"type": "closeAccount", "info": map[string]interface{}{"account": "temp-dest", "destination": "owner", "owner": "owner"}}},
			}
			meta := detail["meta"].(map[string]interface{})
			if inner {
				meta["innerInstructions"] = []interface{}{map[string]interface{}{"index": 0, "instructions": extra}}
			} else {
				message["instructions"] = append(message["instructions"].([]interface{}), extra...)
			}
			meta["preTokenBalances"] = []interface{}{
				map[string]interface{}{"accountIndex": 2, "mint": "So11111111111111111111111111111111111111112", "uiTokenAmount": map[string]interface{}{"amount": "1000000000", "decimals": 9}},
				map[string]interface{}{"accountIndex": 3, "mint": "So11111111111111111111111111111111111111112", "uiTokenAmount": map[string]interface{}{"amount": "0", "decimals": 9}},
			}
			meta["postTokenBalances"] = []interface{}{map[string]interface{}{"accountIndex": 1, "mint": USDT_Mint, "uiTokenAmount": map[string]interface{}{"amount": "3000000", "decimals": 6}}}
			scanSolCursor()
			assertSolCursor(t, l, "B")
			assertSolCursorSettlement(t, o)
			if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
				t.Fatal(err)
			}
			clearProcessedSignatures()
			scanSolCursor()
			assertSolCursorSettlement(t, o)
		})
	}
}

func TestSolCursorUncheckedZeroDecimals(t *testing.T) {
	l := setupSolCursorLedger(t)
	o := solCursorOrder(t)
	if err := dao.Mdb.Model(o).Update("asset_decimals", 0).Error; err != nil {
		t.Fatal(err)
	}
	message, meta, row := solUncheckedPayment(l)
	message["instructions"].([]interface{})[0].(map[string]interface{})["parsed"].(map[string]interface{})["info"].(map[string]interface{})["amount"] = "3"
	row["uiTokenAmount"] = map[string]interface{}{"amount": "3", "decimals": 0}
	meta["postTokenBalances"] = []interface{}{row}
	scanSolCursor()
	assertSolCursor(t, l, "B")
	assertSolCursorSettlement(t, o)
}

func TestSolCursorUncheckedSourceBalance(t *testing.T) {
	for _, field := range []string{"preTokenBalances", "postTokenBalances"} {
		t.Run(field, func(t *testing.T) {
			l := setupSolCursorLedger(t)
			o := solCursorOrder(t)
			_, meta, row := solUncheckedPayment(l)
			row["accountIndex"] = 0
			meta[field] = []interface{}{row}
			scanSolCursor()
			assertSolCursor(t, l, "B")
			assertSolCursorSettlement(t, o)
		})
	}
}
