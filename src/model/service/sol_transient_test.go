package service

import (
	"fmt"
	"testing"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/gagliardetto/solana-go"
)

var solTransientSource = solana.PublicKey{21}.String()
var solTransientDestination = solana.PublicKey{22}.String()

// These accounts do not exist at either transaction boundary. No RPC retry
// can supply their pre/post token balances, even though the response is complete.
func solTransientInstructions() []interface{} {
	ix := func(kind string, info map[string]interface{}) interface{} {
		return map[string]interface{}{"programId": TokenProgramID, "parsed": map[string]interface{}{"type": kind, "info": info}}
	}
	var instructions []interface{}
	for _, account := range []string{solTransientSource, solTransientDestination} {
		instructions = append(instructions, map[string]interface{}{"programId": SystemProgramID, "parsed": map[string]interface{}{"type": "createAccount", "info": map[string]interface{}{"newAccount": account, "source": "payer", "lamports": 2000000000, "space": 165, "owner": TokenProgramID}}})
		instructions = append(instructions, ix("initializeAccount3", map[string]interface{}{"account": account, "mint": "So11111111111111111111111111111111111111112", "owner": "payer"}))
	}
	instructions = append(instructions, ix("transfer", map[string]interface{}{"source": solTransientSource, "destination": solTransientDestination, "amount": "1000000000"}))
	for _, account := range []string{solTransientSource, solTransientDestination} {
		instructions = append(instructions, ix("closeAccount", map[string]interface{}{"account": account, "destination": "payer", "owner": "payer"}))
	}
	return instructions
}

func TestSolCursorTransientAccounts(t *testing.T) {
	for _, inner := range []bool{false, true} {
		for _, checked := range []bool{false, true} {
			t.Run(fmt.Sprintf("inner=%v/checked=%v", inner, checked), func(t *testing.T) {
				l := setupSolCursorLedger(t)
				o := solCursorOrder(t)
				message, meta, row := solUncheckedPayment(l)
				payment := message["instructions"].([]interface{})[0].(map[string]interface{})
				if checked {
					payment["parsed"] = map[string]interface{}{"type": "transferChecked", "info": map[string]interface{}{"source": "source-ata", "destination": l.ata, "mint": USDT_Mint, "tokenAmount": map[string]interface{}{"amount": "3000000", "decimals": 6}}}
				}
				message["accountKeys"] = append(message["accountKeys"].([]interface{}), map[string]interface{}{"pubkey": solTransientSource}, map[string]interface{}{"pubkey": solTransientDestination})
				meta["preTokenBalances"] = []interface{}{}
				meta["postTokenBalances"] = []interface{}{row}
				extra := solTransientInstructions()
				if inner {
					// Keep a real outer invocation so CPI event indexes are stable.
					message["instructions"] = []interface{}{map[string]interface{}{"programId": "custom-program", "accounts": []interface{}{}, "data": "1"}}
					meta["innerInstructions"] = []interface{}{map[string]interface{}{"index": 0, "instructions": append(extra, payment)}}
				} else {
					message["instructions"] = append(extra, payment)
				}
				if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
					t.Fatal(err)
				}
				// Even with an unrelated transient transfer, broken metadata for
				// the actual payment must retain the cursor and write no receipts.
				if checked {
					delete(payment["parsed"].(map[string]interface{})["info"].(map[string]interface{})["tokenAmount"].(map[string]interface{}), "decimals")
				} else {
					delete(row["uiTokenAmount"].(map[string]interface{}), "decimals")
				}
				scanSolCursor()
				assertSolCursor(t, l, "A")
				var count int64
				if err := dao.Mdb.Model(&mdb.PaymentReceipt{}).Count(&count).Error; err != nil || count != 0 {
					t.Fatalf("incomplete payment consumed: %d %v", count, err)
				}
				if checked {
					payment["parsed"].(map[string]interface{})["info"].(map[string]interface{})["tokenAmount"].(map[string]interface{})["decimals"] = 6
				} else {
					row["uiTokenAmount"].(map[string]interface{})["decimals"] = 6
				}
				clearProcessedSignatures()
				data.ResetRpcEndpointPolicyForTest()
				data.ResetRpcFailoverForTest()
				scanSolCursor()
				assertSolCursor(t, l, "B")
				assertSolCursorSettlement(t, o)
				var receipt mdb.PaymentReceipt
				if err := dao.Mdb.First(&receipt).Error; err != nil {
					t.Fatal(err)
				}
				index := len(extra)
				if inner {
					index++
				}
				if receipt.EventID != fmt.Sprintf("instruction:%d", index) {
					t.Fatalf("event identity shifted: %s", receipt.EventID)
				}
				if err := data.SavePaymentCursor("solana:signatures:"+l.ata, "A"); err != nil {
					t.Fatal(err)
				}
				clearProcessedSignatures()
				data.ResetRpcEndpointPolicyForTest()
				data.ResetRpcFailoverForTest()
				scanSolCursor()
				assertSolCursorSettlement(t, o)
			})
		}
	}
}
