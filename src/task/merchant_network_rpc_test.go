//go:build merchant_e2e

package task_test

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/task"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
)

// Only chain servers are synthetic. Scanning, parsing, validation, receipts,
// callback persistence and both applications' order/credit handlers are real.
type networkE2ERPC struct {
	mu                                           sync.Mutex
	t                                            *testing.T
	network, owner, asset, ata, fault, signature string
	amount, version                              int64
	stamp                                        time.Time
	tronBlocks                                   map[int64]map[string]interface{}
}

func seedNetworkE2EAssets(t *testing.T) map[string]string {
	t.Helper()
	networkE2EMust(t, dao.Mdb.Create(&mdb.Chain{Network: mdb.NetworkAptos, Enabled: true, MinConfirmations: 2}).Error)
	networkE2EMust(t, dao.Mdb.Model(&mdb.Chain{}).Where("network = ?", mdb.NetworkTron).Update("min_confirmations", 2).Error)
	// The asset identity is configured, as in deployment; the fake Aptos node
	// represents a six-decimal FA. This does not attest a live mainnet contract.
	for _, asset := range []mdb.ChainToken{
		{Network: mdb.NetworkAptos, Symbol: "USDT", ContractAddress: "0x" + strings.Repeat("0", 63) + "1", Decimals: 6, Enabled: true},
		{Network: mdb.NetworkSolana, Symbol: "USDT", ContractAddress: service.USDT_Mint, Decimals: 6, Enabled: true},
	} {
		networkE2EMust(t, dao.Mdb.Create(&asset).Error)
	}
	tron, err := task.HexToTronAddress(strings.Repeat("1", 40))
	networkE2EMust(t, err)
	owners := map[string]string{mdb.NetworkTron: tron, mdb.NetworkAptos: "0x" + strings.Repeat("0", 63) + "a", mdb.NetworkSolana: "2uFTf9TZ8gd7Kg6hkb79TxfaeNpaAgpJ8uVHguv2Yweu"}
	for network, owner := range owners {
		networkE2EMust(t, dao.Mdb.Create(&mdb.WalletAddress{Network: network, Address: owner, Status: 1}).Error)
	}
	return owners
}
func newNetworkE2ERPC(t *testing.T, network, owner string, amount int64, stamp time.Time) *networkE2ERPC {
	t.Helper()
	asset, err := data.GetEnabledChainTokenBySymbol(network, "USDT")
	networkE2EMust(t, err)
	f := &networkE2ERPC{t: t, network: network, owner: owner, asset: asset.ContractAddress, amount: amount, version: 2, stamp: stamp, signature: strings.ReplaceAll(t.Name(), "/", "-")}
	if network == mdb.NetworkSolana {
		f.ata, err = service.FindATAAddress(owner, asset.ContractAddress)
		networkE2EMust(t, err)
	}
	f.rebuildTron()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	networkE2EMust(t, dao.Mdb.Create(&mdb.RpcNode{Network: network, Type: mdb.RpcNodeTypeHttp, Url: server.URL, Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposeGeneral}).Error)
	if network == mdb.NetworkAptos {
		networkE2EMust(t, dao.Mdb.Create(&mdb.RpcNode{Network: network, Type: mdb.RpcNodeTypeIndexer, Url: server.URL + "/graphql", Enabled: true, Status: mdb.RpcNodeStatusOk, Purpose: mdb.RpcNodePurposeGeneral}).Error)
	}
	return f
}
func (f *networkE2ERPC) setFault(fault string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fault
	f.rebuildTron()
}
func (f *networkE2ERPC) recover(newTransaction bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if newTransaction {
		f.version++
		f.signature += "-valid"
	}
	f.fault = ""
	f.rebuildTron()
}
func (f *networkE2ERPC) resetCursor() error {
	switch f.network {
	case mdb.NetworkTron:
		return data.SavePaymentCursor("tron:blocks", "1")
	case mdb.NetworkAptos:
		b, _ := json.Marshal(service.AptosEventPosition{Version: 1, Index: math.MaxInt64})
		return data.SavePaymentCursor("aptos:deposits:"+f.owner, string(b))
	case mdb.NetworkSolana:
		return data.SavePaymentCursor("solana:signatures:"+f.ata, "before-payment")
	}
	return nil
}
func (f *networkE2ERPC) assertInitialCursor(t *testing.T) {
	t.Helper()
	key, want := "tron:blocks", "1"
	if f.network == mdb.NetworkAptos {
		key = "aptos:deposits:" + f.owner
		b, _ := json.Marshal(service.AptosEventPosition{Version: 1, Index: math.MaxInt64})
		want = string(b)
	}
	if f.network == mdb.NetworkSolana {
		key = "solana:signatures:" + f.ata
		want = "before-payment"
	}
	got, err := data.LoadPaymentCursor(key)
	networkE2EMust(t, err)
	if got != want {
		t.Fatalf("failed scan advanced cursor %s: %s -> %s", key, want, got)
	}
}
func (f *networkE2ERPC) rebuildTron() {
	if f.network != mdb.NetworkTron {
		return
	}
	recipient, err := task.TronAddressToHex(f.owner)
	networkE2EMust(f.t, err)
	contract, err := task.TronAddressToHex(f.asset)
	networkE2EMust(f.t, err)
	if f.fault == "wrong-token" {
		contract = "41" + strings.Repeat("3", 40)
	}
	if f.fault == "wrong-recipient" {
		recipient = "41" + strings.Repeat("4", 40)
	}
	result := "SUCCESS"
	if f.fault == "failed-chain-tx" {
		result = "REVERT"
	}
	f.tronBlocks = map[int64]map[string]interface{}{}
	for n := int64(1); n <= f.version+1; n++ {
		txs := []interface{}{}
		if n == f.version {
			txs = append(txs, map[string]interface{}{"ret": []interface{}{map[string]interface{}{"contractRet": result}}, "raw_data": map[string]interface{}{"timestamp": f.stamp.UnixMilli() + n, "contract": []interface{}{map[string]interface{}{"type": "TriggerSmartContract", "parameter": map[string]interface{}{"value": map[string]interface{}{"owner_address": "41" + strings.Repeat("2", 40), "contract_address": contract, "data": task.TransferMethodID + strings.Repeat("0", 24) + recipient[2:] + fmt.Sprintf("%064x", f.amount*1_000_000)}}}}}})
		}
		block := map[string]interface{}{"block_header": map[string]interface{}{"raw_data": map[string]interface{}{"number": n, "timestamp": f.stamp.Add(time.Duration(n-f.version) * time.Second).UnixMilli()}}, "transactions": txs}
		sealTronTestBlock(f.t, block)
		f.tronBlocks[n] = block
	}
}
func (f *networkE2ERPC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if f.fault == "rpc-unavailable" {
		w.WriteHeader(503)
		fmt.Fprint(w, `{"error":"injected outage"}`)
		return
	}
	switch f.network {
	case mdb.NetworkTron:
		n := f.version + 1
		if f.fault == "unconfirmed" {
			n = f.version
		}
		if r.URL.Path == "/wallet/getblockbynum" {
			var q struct{ Num int64 }
			_ = json.NewDecoder(r.Body).Decode(&q)
			n = q.Num
		} else if r.URL.Path != "/wallet/getnowblock" {
			http.Error(w, "unexpected TRON path", 400)
			return
		}
		block := f.tronBlocks[n]
		if f.fault == "incomplete-rpc" && n == f.version {
			raw, _ := json.Marshal(block)
			var broken map[string]interface{}
			_ = json.Unmarshal(raw, &broken)
			delete(broken, "transactions")
			json.NewEncoder(w).Encode(broken)
			return
		}
		json.NewEncoder(w).Encode(block)
	case mdb.NetworkAptos:
		f.serveAptos(w, r)
	case mdb.NetworkSolana:
		f.serveSolana(w, r)
	}
}
func (f *networkE2ERPC) serveAptos(w http.ResponseWriter, r *http.Request) {
	asset := f.asset
	owner := f.owner
	if f.fault == "wrong-token" {
		asset = "0x" + strings.Repeat("0", 63) + "9"
	}
	if f.fault == "wrong-recipient" {
		owner = "0x" + strings.Repeat("0", 63) + "b"
	}
	if r.URL.Path == "/graphql" {
		var q struct {
			Query     string
			Variables map[string]interface{}
		}
		_ = json.NewDecoder(r.Body).Decode(&q)
		head := f.version + 1
		if f.fault == "unconfirmed" {
			head = f.version
		}
		response := map[string]interface{}{"ledger_infos": []interface{}{map[string]int{"chain_id": 1}}, "processor_status": []interface{}{map[string]int64{"last_success_version": head}}}
		if strings.Contains(q.Query, "PaymentDeposits") {
			v, _ := strconv.ParseInt(fmt.Sprint(q.Variables["version"]), 10, 64)
			idx, _ := strconv.ParseInt(fmt.Sprint(q.Variables["event"]), 10, 64)
			bound, _ := strconv.ParseInt(fmt.Sprint(q.Variables["head"]), 10, 64)
			rows := []interface{}{}
			// A successful-deposit indexer never returns failed transactions. An RPC
			// failure inconsistent with the indexer is separately exercised below.
			if owner == fmt.Sprint(q.Variables["owner"]) && f.version <= bound && (f.version > v || f.version == v && idx < 1) {
				rows = append(rows, map[string]interface{}{"transaction_version": f.version, "event_index": 1, "owner_address": owner, "asset_type": asset, "type": "0x1::fungible_asset::Deposit", "amount": fmt.Sprint(f.amount * 1_000_000)})
			}
			response["fungible_asset_activities"] = rows
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"data": response})
		return
	}
	if r.URL.Path != fmt.Sprintf("/v1/transactions/by_version/%d", f.version) {
		http.Error(w, "unexpected Aptos RPC path "+r.URL.Path, 400)
		return
	}
	body := aptosFungibleTransferBody(f.t, "0x"+fmt.Sprintf("%064x", f.version), f.version, owner, asset, fmt.Sprint(f.amount*1_000_000))
	var txs []map[string]interface{}
	_ = json.Unmarshal(body, &txs)
	tx := txs[0]
	tx["timestamp"] = fmt.Sprint(f.stamp.UnixMicro())
	if f.fault == "failed-chain-tx" {
		tx["success"] = false
	}
	if f.fault == "incomplete-rpc" {
		delete(tx, "changes")
	}
	json.NewEncoder(w).Encode(tx)
}
func (f *networkE2ERPC) serveSolana(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Method string
		Params []json.RawMessage
	}
	_ = json.NewDecoder(r.Body).Decode(&q)
	var target string
	if len(q.Params) > 0 {
		_ = json.Unmarshal(q.Params[0], &target)
	}
	var result interface{}
	switch q.Method {
	case "getSignaturesForAddress":
		var opts struct{ Before, Until, Commitment string }
		if len(q.Params) > 1 {
			_ = json.Unmarshal(q.Params[1], &opts)
		}
		if opts.Commitment != "finalized" {
			http.Error(w, "nonfinalized query", 400)
			return
		}
		rows := []interface{}{}
		if target == f.ata && opts.Until != f.signature && opts.Before == "" && f.fault != "unconfirmed" {
			rows = append(rows, map[string]interface{}{"signature": f.signature, "slot": f.version, "blockTime": f.stamp.Unix(), "err": nil})
		}
		result = rows
	case "getTransaction":
		mint, destination := f.asset, f.ata
		if f.fault == "wrong-token" {
			mint = service.USDC_Mint
		}
		if f.fault == "wrong-recipient" {
			destination = "11111111111111111111111111111111"
		}
		meta := map[string]interface{}{"err": nil}
		if f.fault == "failed-chain-tx" {
			meta["err"] = map[string]interface{}{"InstructionError": []interface{}{0, "InvalidArgument"}}
		}
		result = map[string]interface{}{"slot": f.version, "blockTime": f.stamp.Unix(), "meta": meta, "transaction": map[string]interface{}{"signatures": []string{f.signature}, "message": map[string]interface{}{"instructions": []interface{}{map[string]interface{}{"programId": service.TokenProgramID, "parsed": map[string]interface{}{"type": "transferChecked", "info": map[string]interface{}{"source": "source-ata", "destination": destination, "mint": mint, "tokenAmount": map[string]interface{}{"amount": fmt.Sprint(f.amount * 1_000_000), "decimals": 6}}}}}}}}
		if f.fault == "incomplete-rpc" {
			delete(result.(map[string]interface{}), "transaction")
		}
	default:
		http.Error(w, "unexpected Solana method "+q.Method, 400)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": result})
}

func aptosFungibleTransferBody(t *testing.T, hash string, version int64, receive string, metadata string, rawAmount string) []byte {
	t.Helper()
	store, err := addressutil.NormalizeMoveAddress(fmt.Sprintf("0x%x", version+1000))
	if err != nil {
		t.Fatalf("normalize store: %v", err)
	}
	senderStore, err := addressutil.NormalizeMoveAddress(fmt.Sprintf("0x%x", version+2000))
	if err != nil {
		t.Fatalf("normalize sender store: %v", err)
	}
	return []byte(`[
		{
			"type":"user_transaction",
			"success":true,
			"hash":"` + hash + `",
			"version":"` + fmt.Sprintf("%d", version) + `",
			"timestamp":"` + fmt.Sprintf("%d", time.Now().UnixMicro()) + `",
			"events":[
				{"type":"0x1::fungible_asset::Withdraw","data":{"amount":"` + rawAmount + `","store":"` + senderStore + `"}},
				{"type":"0x1::fungible_asset::Deposit","data":{"amount":"` + rawAmount + `","store":"` + store + `"}}
			],
			"changes":[
				{"address":"` + senderStore + `","data":{"type":"0x1::fungible_asset::FungibleStore","data":{"metadata":{"inner":"` + metadata + `"}}},"type":"write_resource"},
				{"address":"` + store + `","data":{"type":"0x1::fungible_asset::FungibleStore","data":{"metadata":{"inner":"` + metadata + `"}}},"type":"write_resource"},
				{"address":"` + store + `","data":{"type":"0x1::object::ObjectCore","data":{"owner":"` + receive + `"}},"type":"write_resource"}
			]
		}
	]`)
}

func (f *networkE2ERPC) assertCompletedCursor(t *testing.T) {
	t.Helper()
	key, want := "tron:blocks", fmt.Sprint(f.version)
	if f.network == mdb.NetworkAptos {
		key = "aptos:deposits:" + f.owner
		b, _ := json.Marshal(service.AptosEventPosition{Version: f.version, Index: math.MaxInt64})
		want = string(b)
	}
	if f.network == mdb.NetworkSolana {
		key = "solana:signatures:" + f.ata
		want = f.signature
	}
	got, err := data.LoadPaymentCursor(key)
	networkE2EMust(t, err)
	if got != want {
		t.Fatalf("scan did not durably complete %s: got=%s want=%s", key, got, want)
	}
}
