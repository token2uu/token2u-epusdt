package service

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	tron "github.com/GMWalletApp/epusdt/crypto"
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/request"
	addressutil "github.com/GMWalletApp/epusdt/util/address"
	"github.com/GMWalletApp/epusdt/util/constant"
	"github.com/GMWalletApp/epusdt/util/math"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/jetton"
)

const manualVerifyRequestTimeout = 15 * time.Second

var erc20TransferEventHash = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

var (
	manualOrderPaymentValidatorMu sync.Mutex
	manualOrderPaymentValidator   manualOrderPaymentValidatorFunc = validateManualOrderPaymentDefault
)

type manualOrderPaymentValidatorFunc func(*mdb.Orders, string, ...*request.ManualPaymentEvidence) (string, error)

// ManualPaymentValidationResult contains the result of manual payment validation.
type ManualPaymentValidationResult struct {
	CanonicalTxID string
	PaidAmount    float64                        // actual crypto amount paid, 0 if exact match
	Evidence      *request.ManualPaymentEvidence // verified chain evidence for every successful payment
}

// ValidateManualOrderPayment verifies that the supplied chain transaction
// really settles the order before an admin manually marks it paid. It returns
// the canonical transaction id that should be persisted for duplicate checks.
func ValidateManualOrderPayment(order *mdb.Orders, blockTransactionID string) (string, error) {
	manualOrderPaymentValidatorMu.Lock()
	validator := manualOrderPaymentValidator
	manualOrderPaymentValidatorMu.Unlock()
	return validator(order, blockTransactionID)
}

// ValidateManualOrderPaymentWithPaidAmount verifies the transaction and returns
// both the canonical tx ID and the actual paid amount (for EPay orders with amount mismatch).
func ValidateManualOrderPaymentWithPaidAmount(order *mdb.Orders, blockTransactionID string) (*ManualPaymentValidationResult, error) {
	manualOrderPaymentValidatorMu.Lock()
	validator := manualOrderPaymentValidator
	manualOrderPaymentValidatorMu.Unlock()
	evidence := &request.ManualPaymentEvidence{}
	canonicalTxID, err := validator(order, blockTransactionID, evidence)
	if err == nil {
		if evidence.RawAmount == "" || evidence.BlockTimeMs <= 0 {
			return nil, fmt.Errorf("verified payment evidence missing")
		}
		evidence.Network, evidence.Token, evidence.Address = order.Network, order.Token, order.ReceiveAddress
		return &ManualPaymentValidationResult{
			CanonicalTxID: canonicalTxID,
			PaidAmount:    0,
			Evidence:      evidence,
		}, nil
	}

	var mismatchErr *AmountMismatchError
	if !errors.As(err, &mismatchErr) {
		return nil, err
	}

	isPackageEpay := data.IsPackageEpayNotifyURL(order.NotifyUrl)
	isEpay := data.IsEpayNotifyURL(order.NotifyUrl)

	switch {
	case isPackageEpay:
		return nil, err

	case isEpay:
		// Zero is also the legacy exact-match sentinel. A mismatch must carry
		// positive raw evidence so it cannot be interpreted as full payment.
		if mismatchErr.Evidence == nil || mismatchErr.ActualPaidAmount <= 0 {
			return nil, fmt.Errorf("non-positive or missing verified payment amount")
		}
		raw, ok := new(big.Int).SetString(mismatchErr.Evidence.RawAmount, 10)
		if !ok || raw.Sign() <= 0 || mismatchErr.Evidence.BlockTimeMs <= 0 {
			return nil, fmt.Errorf("invalid verified payment evidence")
		}
		// The verifier must return the identity from the verified chain evidence,
		// even when only the amount differs. Never fall back to the user's spelling.
		if canonicalTxID == "" {
			return nil, fmt.Errorf("verified payment identity missing")
		}
		evidence := *mismatchErr.Evidence
		evidence.Network, evidence.Token, evidence.Address = order.Network, order.Token, order.ReceiveAddress
		canonicalTxID = data.CanonicalPaymentTransactionID(order.Network, canonicalTxID)
		if err := ensureManualBlockTransactionUnused(order, canonicalTxID, mismatchErr.Evidence.EventID); err != nil {
			return nil, err
		}
		return &ManualPaymentValidationResult{
			CanonicalTxID: canonicalTxID,
			PaidAmount:    mismatchErr.ActualPaidAmount,
			Evidence:      &evidence,
		}, nil

	default:
		return nil, err
	}
}

// SetManualOrderPaymentValidatorForTest swaps the chain verifier in tests so
// route/controller tests don't depend on public RPC availability.
func SetManualOrderPaymentValidatorForTest(fn func(*mdb.Orders, string, *request.ManualPaymentEvidence) (string, error)) func() {
	manualOrderPaymentValidatorMu.Lock()
	old := manualOrderPaymentValidator
	if fn == nil {
		manualOrderPaymentValidator = validateManualOrderPaymentDefault
	} else {
		manualOrderPaymentValidator = func(order *mdb.Orders, blockTransactionID string, evidence ...*request.ManualPaymentEvidence) (string, error) {
			sink := &request.ManualPaymentEvidence{}
			if len(evidence) > 0 {
				sink = evidence[0]
			}
			return fn(order, blockTransactionID, sink)
		}
	}
	manualOrderPaymentValidatorMu.Unlock()
	return func() {
		manualOrderPaymentValidatorMu.Lock()
		manualOrderPaymentValidator = old
		manualOrderPaymentValidatorMu.Unlock()
	}
}

func validateManualOrderPaymentDefault(order *mdb.Orders, blockTransactionID string, evidence ...*request.ManualPaymentEvidence) (string, error) {
	if len(evidence) == 0 || evidence[0] == nil {
		evidence = []*request.ManualPaymentEvidence{{}}
	}
	if order == nil || order.ID == 0 {
		return "", fmt.Errorf("order not found")
	}
	txID := strings.TrimSpace(blockTransactionID)
	if txID == "" {
		return "", fmt.Errorf("block_transaction_id is required")
	}

	var canonicalTxID string
	var err error
	switch strings.ToLower(strings.TrimSpace(order.Network)) {
	case mdb.NetworkTron:
		canonicalTxID, err = validateManualTronPayment(order, txID, evidence...)
	case mdb.NetworkSolana:
		canonicalTxID, err = validateManualSolanaPayment(order, txID, evidence...)
	case mdb.NetworkTon:
		canonicalTxID, err = validateManualTonPayment(order, txID, evidence...)
	case mdb.NetworkAptos:
		canonicalTxID, err = ValidateManualAptosPayment(order, txID, evidence...)
	case mdb.NetworkEthereum, mdb.NetworkBsc, mdb.NetworkPolygon, mdb.NetworkPlasma:
		canonicalTxID, err = validateManualEvmPayment(order, txID, evidence...)
	default:
		return "", fmt.Errorf("unsupported manual payment verification network: %s", order.Network)
	}
	if err != nil {
		return canonicalTxID, err
	}
	if err = ensureManualBlockTransactionUnused(order, canonicalTxID, evidence[0].EventID); err != nil {
		return "", err
	}
	return canonicalTxID, nil
}

func ensureManualBlockTransactionUnused(order *mdb.Orders, canonicalTxID string, eventIDs ...string) error {
	eventID := ""
	if len(eventIDs) > 0 {
		eventID = eventIDs[0]
	}
	data.CollectionMu.Lock()
	claimed, err := data.PaymentEventAlreadyClaimed(dao.Mdb, order.Network, canonicalTxID, eventID)
	data.CollectionMu.Unlock()
	if err != nil {
		return err
	}
	if claimed {
		return constant.OrderBlockAlreadyProcess
	}
	return nil
}

func validateManualEvmPayment(order *mdb.Orders, txID string, evidence ...*request.ManualPaymentEvidence) (string, error) {
	txHash, canonicalTxID, err := canonicalEvmHash(txID)
	if err != nil {
		return "", err
	}

	token, err := data.GetEnabledChainTokenBySymbol(order.Network, order.Token)
	if err != nil {
		return "", err
	}
	if token == nil || token.ID == 0 || strings.TrimSpace(token.ContractAddress) == "" {
		return "", fmt.Errorf("enabled token contract not configured for %s/%s", order.Network, order.Token)
	}

	ctx, cancel := context.WithTimeout(context.Background(), manualVerifyRequestTimeout)
	defer cancel()

	clients, err := dialManualEvmClients(ctx, order.Network)
	if err != nil {
		return "", err
	}
	defer closeManualEvmClients(clients)

	if err = validateManualEvmPaymentAcrossClients(ctx, clients, order, txHash, token, evidence...); err != nil {
		return canonicalTxID, err
	}
	return canonicalTxID, nil
}

type evmChainReader interface {
	TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

type manualEvmClient struct {
	label  string
	reader evmChainReader
	close  func()
}

func closeManualEvmClients(clients []manualEvmClient) {
	for _, item := range clients {
		if item.close != nil {
			item.close()
		}
	}
}

func validateManualEvmPaymentAcrossClients(ctx context.Context, clients []manualEvmClient, order *mdb.Orders, txHash common.Hash, token *mdb.ChainToken, evidence ...*request.ManualPaymentEvidence) error {
	var verifyErrors []string
	var mismatchErr *AmountMismatchError
	for _, item := range clients {
		if err := validateManualEvmPaymentWithClient(ctx, item.reader, order, txHash, token, evidence...); err != nil {
			if mismatchErr == nil {
				errors.As(err, &mismatchErr)
			}
			verifyErrors = append(verifyErrors, fmt.Sprintf("%s: %v", item.label, err))
			continue
		}
		return nil
	}
	if mismatchErr != nil {
		return mismatchErr
	}
	if len(verifyErrors) > 0 {
		return fmt.Errorf("manual EVM verification failed: %s", strings.Join(verifyErrors, "; "))
	}
	return fmt.Errorf("no enabled %s WS/HTTP RPC node configured", order.Network)
}

func validateManualEvmPaymentWithClient(ctx context.Context, client evmChainReader, order *mdb.Orders, txHash common.Hash, token *mdb.ChainToken, evidence ...*request.ManualPaymentEvidence) error {
	receipt, err := client.TransactionReceipt(ctx, txHash)
	if err != nil {
		return fmt.Errorf("fetch transaction receipt: %w", err)
	}
	if receipt == nil || receipt.TxHash != txHash || receipt.BlockHash == (common.Hash{}) {
		return fmt.Errorf("transaction receipt identity does not match request")
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return fmt.Errorf("transaction is not successful")
	}
	if receipt.BlockNumber == nil || !receipt.BlockNumber.IsUint64() {
		return fmt.Errorf("transaction receipt missing or invalid block number")
	}
	txHeader, err := client.HeaderByNumber(ctx, receipt.BlockNumber)
	if err != nil {
		return fmt.Errorf("fetch transaction block header: %w", err)
	}
	if txHeader == nil || txHeader.Number == nil || txHeader.Number.Cmp(receipt.BlockNumber) != 0 ||
		txHeader.Hash() != receipt.BlockHash || txHeader.Time == 0 || txHeader.Time > uint64((1<<63-1)/1000) {
		return fmt.Errorf("transaction receipt does not match canonical block header")
	}
	if err = ensureEvmTransactionNotBeforeOrder(txHeader.Time, order); err != nil {
		return err
	}
	if err = ensureEvmConfirmations(ctx, client, order.Network, receipt.BlockNumber); err != nil {
		return err
	}

	contract, err := normalizeEvmAddress(token.ContractAddress)
	if err != nil {
		return fmt.Errorf("invalid token contract address: %w", err)
	}
	to, err := normalizeEvmAddress(order.ReceiveAddress)
	if err != nil {
		return fmt.Errorf("invalid order receive address: %w", err)
	}
	var mismatch *AmountMismatchError
	for _, item := range receipt.Logs {
		if item == nil || !strings.EqualFold(item.Address.Hex(), contract.Hex()) {
			continue
		}
		if len(item.Topics) == 0 || item.Topics[0] != erc20TransferEventHash {
			continue
		}
		if err := ValidateEvmTransferLog(*item); err != nil {
			return err
		}
		if !strings.EqualFold(common.BytesToAddress(item.Topics[2].Bytes()).Hex(), to.Hex()) {
			continue
		}
		if item.Removed || item.TxHash != txHash || item.BlockHash != receipt.BlockHash || item.BlockNumber != receipt.BlockNumber.Uint64() {
			return fmt.Errorf("transfer log does not match transaction receipt")
		}
		rawAmount := new(big.Int).SetBytes(item.Data)
		if manualOrderAmountMatches(order, rawAmount, token.Decimals) {
			captureManualEvidence(evidence, rawAmount, token.Decimals, int64(txHeader.Time)*1000, token.ContractAddress, fmt.Sprintf("log:%d", item.Index))
			return nil
		}
		mismatch = newAmountMismatch(rawAmount, token.Decimals, int64(txHeader.Time)*1000, token.ContractAddress, fmt.Sprintf("log:%d", item.Index))
	}

	if mismatch != nil {
		return mismatch
	}
	return fmt.Errorf("matching token transfer to order address not found")
}

func dialManualEvmClients(ctx context.Context, network string) ([]manualEvmClient, error) {
	var clients []manualEvmClient
	var connectErrors []string

	nodes, err := listManualEvmRpcCandidates(network)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if strings.TrimSpace(node.Url) == "" {
			continue
		}
		client, err := dialManualEvmClient(ctx, node)
		if err == nil {
			clients = append(clients, manualEvmClient{
				label:  manualRpcNodeLabel(node),
				reader: client,
				close:  client.Close,
			})
			continue
		}
		connectErrors = append(connectErrors, fmt.Sprintf("%s: %v", manualRpcNodeLabel(node), err))
	}
	if len(clients) > 0 {
		return clients, nil
	}
	if len(connectErrors) > 0 {
		return nil, fmt.Errorf("connect %s RPC failed: %s", network, strings.Join(connectErrors, "; "))
	}
	return nil, fmt.Errorf("no enabled %s WS/HTTP RPC node configured", network)
}

func listManualEvmRpcCandidates(network string) ([]mdb.RpcNode, error) {
	buckets := make([][]mdb.RpcNode, 4)
	for _, nodeType := range []string{mdb.RpcNodeTypeHttp, mdb.RpcNodeTypeWs} {
		nodes, err := data.ListManualPaymentRpcCandidates(network, nodeType)
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			node.Purpose = data.NormalizeRpcNodePurpose(node.Purpose)
			switch node.Purpose {
			case mdb.RpcNodePurposeGeneral, mdb.RpcNodePurposeBoth:
				if node.Status == mdb.RpcNodeStatusOk {
					buckets[0] = append(buckets[0], node)
				} else {
					buckets[1] = append(buckets[1], node)
				}
			case mdb.RpcNodePurposeManualVerify:
				if node.Status == mdb.RpcNodeStatusOk {
					buckets[2] = append(buckets[2], node)
				} else {
					buckets[3] = append(buckets[3], node)
				}
			}
		}
	}

	out := make([]mdb.RpcNode, 0)
	for _, bucket := range buckets {
		out = append(out, bucket...)
	}
	return out, nil
}

func dialManualEvmClient(ctx context.Context, node mdb.RpcNode) (*ethclient.Client, error) {
	rpcURL := strings.TrimSpace(node.Url)
	return ethclient.DialContext(ctx, rpcURL)
}

func manualRpcNodeLabel(node mdb.RpcNode) string {
	purpose := data.NormalizeRpcNodePurpose(node.Purpose)
	return fmt.Sprintf("%s purpose=%s", data.RpcNodeLogLabel(node), purpose)
}

func ensureEvmTransactionNotBeforeOrder(blockTime uint64, order *mdb.Orders) error {
	if order == nil {
		return fmt.Errorf("order not found")
	}
	if int64(blockTime)*1000 < data.PaymentAssignmentTimeMs(order) {
		return fmt.Errorf("transaction predates the order")
	}
	return nil
}

func ensureEvmConfirmations(ctx context.Context, client evmChainReader, network string, txBlock *big.Int) error {
	chain, err := data.GetChainByNetwork(network)
	if err != nil {
		return err
	}
	minConfirmations := 1
	if chain != nil && chain.MinConfirmations > 0 {
		minConfirmations = chain.MinConfirmations
	}
	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("fetch latest block: %w", err)
	}
	if header == nil || header.Number == nil || !header.Number.IsUint64() {
		return fmt.Errorf("latest block has no valid number")
	}
	confirmations := new(big.Int).Sub(header.Number, txBlock)
	confirmations.Add(confirmations, big.NewInt(1))
	if confirmations.Sign() < 0 || confirmations.Cmp(big.NewInt(int64(minConfirmations))) < 0 {
		return fmt.Errorf("transaction confirmations %s below required %d", confirmations.String(), minConfirmations)
	}
	return nil
}

type tronContractParam struct {
	TypeURL string          `json:"type_url"`
	Value   json.RawMessage `json:"value"`
}

type manualTronTransaction struct {
	TxID    string `json:"txID"`
	RawData struct {
		Contract []struct {
			Type      string            `json:"type"`
			Parameter tronContractParam `json:"parameter"`
		} `json:"contract"`
		Timestamp int64 `json:"timestamp"`
	} `json:"raw_data"`
	Ret []struct {
		ContractRet string `json:"contractRet"`
	} `json:"ret"`
}

type manualTronTxInfo struct {
	ID             string               `json:"id"`
	BlockNumber    int64                `json:"blockNumber"`
	BlockTimeStamp int64                `json:"blockTimeStamp"`
	ContractResult []string             `json:"contractResult"`
	Log            []manualTronEventLog `json:"log"`
	Receipt        struct {
		Result string `json:"result"`
	} `json:"receipt"`
}

type manualTronEventLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type manualTronBlock struct {
	BlockHeader struct {
		RawData struct {
			Number int64 `json:"number"`
		} `json:"raw_data"`
	} `json:"block_header"`
}

type manualTronTransferContractValue struct {
	OwnerAddress string `json:"owner_address"`
	ToAddress    string `json:"to_address"`
	Amount       int64  `json:"amount"`
}

func validateManualTronPayment(order *mdb.Orders, txID string, evidence ...*request.ManualPaymentEvidence) (string, error) {
	normalizedTxID, err := normalizeTronTxID(txID)
	if err != nil {
		return "", err
	}

	nodes, err := data.ListManualPaymentRpcCandidates(mdb.NetworkTron, mdb.RpcNodeTypeHttp)
	if err != nil {
		return "", err
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("no enabled %s %s RPC node configured in rpc_nodes", mdb.NetworkTron, mdb.RpcNodeTypeHttp)
	}

	var verifyErrors []string
	var mismatchErr *AmountMismatchError
	for _, node := range nodes {
		if strings.TrimSpace(node.Url) == "" {
			continue
		}
		if err = validateManualTronPaymentWithNode(order, normalizedTxID, node, evidence...); err != nil {
			if mismatchErr == nil {
				errors.As(err, &mismatchErr)
			}
			verifyErrors = append(verifyErrors, fmt.Sprintf("%s: %v", manualRpcNodeLabel(node), err))
			continue
		}
		return normalizedTxID, nil
	}
	if mismatchErr != nil {
		return normalizedTxID, mismatchErr
	}
	if len(verifyErrors) > 0 {
		return "", fmt.Errorf("manual TRON verification failed: %s", strings.Join(verifyErrors, "; "))
	}
	return "", fmt.Errorf("no enabled %s %s RPC node configured in rpc_nodes", mdb.NetworkTron, mdb.RpcNodeTypeHttp)
}

func validateManualTronPaymentWithNode(order *mdb.Orders, normalizedTxID string, node mdb.RpcNode, evidence ...*request.ManualPaymentEvidence) error {
	baseURL := strings.TrimSpace(node.Url)
	var tx manualTronTransaction
	if err := tronPostJSON(baseURL, node.ApiKey, "/wallet/gettransactionbyid", map[string]interface{}{"value": normalizedTxID}, &tx); err != nil {
		return fmt.Errorf("fetch tron transaction: %w", err)
	}
	if returnedID, err := normalizeTronTxID(tx.TxID); err != nil || returnedID != normalizedTxID {
		return fmt.Errorf("transaction id mismatch")
	}
	if len(tx.Ret) > 0 && tx.Ret[0].ContractRet != "" && tx.Ret[0].ContractRet != "SUCCESS" {
		return fmt.Errorf("transaction is not successful: %s", tx.Ret[0].ContractRet)
	}

	var info manualTronTxInfo
	if err := tronPostJSON(baseURL, node.ApiKey, "/wallet/gettransactioninfobyid", map[string]interface{}{"value": normalizedTxID}, &info); err != nil {
		return fmt.Errorf("fetch tron transaction info: %w", err)
	}
	if returnedID, err := normalizeTronTxID(info.ID); err != nil || returnedID != normalizedTxID {
		return fmt.Errorf("transaction info id mismatch")
	}
	if info.BlockNumber <= 0 {
		return fmt.Errorf("transaction is not confirmed")
	}
	if info.Receipt.Result != "" && info.Receipt.Result != "SUCCESS" {
		return fmt.Errorf("transaction is not successful: %s", info.Receipt.Result)
	}
	if err := ensureTronConfirmations(baseURL, node.ApiKey, order.Network, info.BlockNumber); err != nil {
		return err
	}
	if info.BlockTimeStamp <= 0 {
		return fmt.Errorf("transaction block timestamp missing")
	}
	if info.BlockTimeStamp < data.PaymentAssignmentTimeMs(order) {
		return fmt.Errorf("transaction predates the order")
	}

	if strings.EqualFold(strings.TrimSpace(order.Token), "TRX") {
		return validateManualTronNativeTransfer(order, &tx, info.BlockTimeStamp, evidence...)
	}
	if err := validateManualTronTRC20Transfer(order, &tx, &info, evidence...); err != nil {
		return err
	}
	return nil
}

func validateManualTronNativeTransfer(order *mdb.Orders, tx *manualTronTransaction, blockTimeMs int64, evidence ...*request.ManualPaymentEvidence) error {
	if len(tx.RawData.Contract) == 0 || tx.RawData.Contract[0].Type != "TransferContract" {
		return fmt.Errorf("transaction is not a TRX transfer")
	}
	var val manualTronTransferContractValue
	if err := json.Unmarshal(tx.RawData.Contract[0].Parameter.Value, &val); err != nil {
		return err
	}
	toAddr, err := tronHexToAddress(val.ToAddress)
	if err != nil {
		return fmt.Errorf("invalid TRX recipient address: %w", err)
	}
	if !strings.EqualFold(toAddr, order.ReceiveAddress) {
		return fmt.Errorf("transaction recipient mismatch")
	}
	if !manualOrderAmountMatches(order, big.NewInt(val.Amount), 6) {
		return newAmountMismatch(big.NewInt(val.Amount), 6, blockTimeMs, "")
	}
	captureManualEvidence(evidence, big.NewInt(val.Amount), 6, blockTimeMs, "")
	return nil
}

func validateManualTronTRC20Transfer(order *mdb.Orders, tx *manualTronTransaction, info *manualTronTxInfo, evidence ...*request.ManualPaymentEvidence) error {
	token, err := data.GetEnabledChainTokenBySymbol(mdb.NetworkTron, order.Token)
	if err != nil {
		return err
	}
	if token == nil || token.ID == 0 || strings.TrimSpace(token.ContractAddress) == "" {
		return fmt.Errorf("enabled token contract not configured for tron/%s", order.Token)
	}
	if len(tx.RawData.Contract) == 0 || tx.RawData.Contract[0].Type != "TriggerSmartContract" {
		return fmt.Errorf("transaction is not a TRC20 transfer")
	}
	contractHex, err := tronAddressToHex(token.ContractAddress)
	if err != nil {
		return fmt.Errorf("invalid configured TRC20 contract: %w", err)
	}
	return validateManualTronTRC20TransferEvent(order, info, contractHex, token.Decimals, evidence...)
}

func validateManualTronTRC20TransferEvent(order *mdb.Orders, info *manualTronTxInfo, contractHex string, decimals int, evidence ...*request.ManualPaymentEvidence) error {
	if info == nil || len(info.Log) == 0 {
		return fmt.Errorf("matching TRC20 transfer event to order address not found")
	}
	toHex, err := tronAddressToHex(order.ReceiveAddress)
	if err != nil {
		return fmt.Errorf("invalid order receive address: %w", err)
	}
	transferTopic := strings.TrimPrefix(erc20TransferEventHash.Hex(), "0x")
	asset, err := tronHexToAddress(contractHex)
	if err != nil {
		return err
	}
	var mismatch *AmountMismatchError
	for _, event := range info.Log {
		eventContractHex, err := normalizeTronAddressHex(event.Address)
		if err != nil || eventContractHex != contractHex {
			continue
		}
		if len(event.Topics) < 3 || normalizeEventTopic(event.Topics[0]) != transferTopic {
			continue
		}
		eventToHex, err := tronTopicAddressToHex(event.Topics[2])
		if err != nil || eventToHex != toHex {
			continue
		}
		rawAmount, err := tronEventDataAmount(event.Data)
		if err != nil {
			return fmt.Errorf("invalid TRC20 transfer event amount: %w", err)
		}
		if manualOrderAmountMatches(order, rawAmount, decimals) {
			captureManualEvidence(evidence, rawAmount, decimals, info.BlockTimeStamp, asset)
			return nil
		}
		mismatch = newAmountMismatch(rawAmount, decimals, info.BlockTimeStamp, asset)
	}
	if mismatch != nil {
		return mismatch
	}
	return fmt.Errorf("matching TRC20 transfer event to order address not found")
}

func ensureTronConfirmations(baseURL, apiKey, network string, txBlock int64) error {
	var latest manualTronBlock
	if err := tronPostJSON(baseURL, apiKey, "/wallet/getnowblock", map[string]interface{}{}, &latest); err != nil {
		return fmt.Errorf("fetch latest tron block: %w", err)
	}
	chain, err := data.GetChainByNetwork(network)
	if err != nil {
		return err
	}
	minConfirmations := 1
	if chain != nil && chain.MinConfirmations > 0 {
		minConfirmations = chain.MinConfirmations
	}
	confirmations := latest.BlockHeader.RawData.Number - txBlock + 1
	if confirmations < int64(minConfirmations) {
		return fmt.Errorf("transaction confirmations %d below required %d", confirmations, minConfirmations)
	}
	return nil
}

func tronPostJSON(baseURL, apiKey, path string, body interface{}, out interface{}) error {
	payload, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if apiKey = strings.TrimSpace(apiKey); apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", apiKey)
	}
	client := &http.Client{Timeout: manualVerifyRequestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
	}
	return json.Unmarshal(raw, out)
}

type manualTonTxRef struct {
	ReceiveRaw string
	LT         uint64
	HashHex    string
	HasLT      bool
}

func validateManualTonPayment(order *mdb.Orders, txID string, evidence ...*request.ManualPaymentEvidence) (string, error) {
	ref, err := parseManualTonTxRef(txID)
	if err != nil {
		return "", err
	}
	orderAddr, err := addressutil.ParseTonMainnetAddress(order.ReceiveAddress)
	if err != nil {
		return "", fmt.Errorf("invalid order receive address: %w", err)
	}
	if ref.ReceiveRaw != "" && ref.ReceiveRaw != orderAddr.Raw {
		return "", fmt.Errorf("transaction recipient mismatch")
	}

	nodes, err := data.ListManualPaymentRpcCandidates(mdb.NetworkTon, mdb.RpcNodeTypeLite)
	if err != nil {
		return "", err
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("no enabled %s %s RPC node configured in rpc_nodes", mdb.NetworkTon, mdb.RpcNodeTypeLite)
	}

	var verifyErrors []string
	var mismatchErr *AmountMismatchError
	var mismatchID string
	for _, node := range nodes {
		api, closeFn, err := dialManualTonClient(node)
		if err != nil {
			verifyErrors = append(verifyErrors, fmt.Sprintf("%s: connect: %v", manualRpcNodeLabel(node), err))
			continue
		}
		canonicalID, err := validateManualTonPaymentWithAPI(context.Background(), api, order, orderAddr.Address, ref, evidence...)
		closeFn()
		if err != nil {
			if mismatchErr == nil {
				if errors.As(err, &mismatchErr) {
					mismatchID = canonicalID
				}
			}
			verifyErrors = append(verifyErrors, fmt.Sprintf("%s: %v", manualRpcNodeLabel(node), err))
			continue
		}
		return canonicalID, nil
	}
	if mismatchErr != nil {
		return mismatchID, mismatchErr
	}
	if len(verifyErrors) > 0 {
		return "", fmt.Errorf("manual TON verification failed: %s", strings.Join(verifyErrors, "; "))
	}
	return "", fmt.Errorf("no enabled %s %s RPC node configured in rpc_nodes", mdb.NetworkTon, mdb.RpcNodeTypeLite)
}

func parseManualTonTxRef(input string) (manualTonTxRef, error) {
	raw := strings.TrimSpace(input)
	if raw == "" {
		return manualTonTxRef{}, fmt.Errorf("block_transaction_id is required")
	}
	if strings.HasPrefix(strings.ToLower(raw), "ton:") {
		body := raw[4:]
		last := strings.LastIndex(body, ":")
		if last <= 0 || last == len(body)-1 {
			return manualTonTxRef{}, fmt.Errorf("invalid ton canonical transaction id")
		}
		hashHex, err := normalizeTonTxHashHex(body[last+1:])
		if err != nil {
			return manualTonTxRef{}, err
		}
		left := body[:last]
		mid := strings.LastIndex(left, ":")
		if mid <= 0 || mid == len(left)-1 {
			return manualTonTxRef{}, fmt.Errorf("invalid ton canonical transaction id")
		}
		lt, err := parseUint64Strict(left[mid+1:])
		if err != nil {
			return manualTonTxRef{}, fmt.Errorf("invalid ton transaction lt")
		}
		receiveRaw, err := addressutil.TonRawAddressKey(left[:mid])
		if err != nil {
			return manualTonTxRef{}, fmt.Errorf("invalid ton canonical recipient: %w", err)
		}
		return manualTonTxRef{ReceiveRaw: receiveRaw, LT: lt, HashHex: hashHex, HasLT: true}, nil
	}
	if idx := strings.Index(raw, ":"); idx > 0 {
		lt, err := parseUint64Strict(raw[:idx])
		if err != nil {
			return manualTonTxRef{}, fmt.Errorf("invalid ton transaction lt")
		}
		hashHex, err := normalizeTonTxHashHex(raw[idx+1:])
		if err != nil {
			return manualTonTxRef{}, err
		}
		return manualTonTxRef{LT: lt, HashHex: hashHex, HasLT: true}, nil
	}
	hashHex, err := normalizeTonTxHashHex(raw)
	if err != nil {
		return manualTonTxRef{}, err
	}
	return manualTonTxRef{HashHex: hashHex}, nil
}

func normalizeTonTxHashHex(raw string) (string, error) {
	hashHex := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(raw, "0x"), "0X")))
	if len(hashHex) != 64 {
		return "", fmt.Errorf("invalid ton transaction hash length")
	}
	if _, err := hex.DecodeString(hashHex); err != nil {
		return "", fmt.Errorf("invalid ton transaction hash")
	}
	return hashHex, nil
}

func parseUint64Strict(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty integer")
	}
	var out uint64
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		out = out*10 + uint64(ch-'0')
	}
	return out, nil
}

func dialManualTonClient(node mdb.RpcNode) (ton.APIClientWrapped, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), manualVerifyRequestTimeout)
	defer cancel()
	return ConnectTonLiteAPI(ctx, node.Url, manualVerifyRequestTimeout, 3)
}

func validateManualTonPaymentWithAPI(ctx context.Context, api ton.APIClientWrapped, order *mdb.Orders, receive *address.Address, ref manualTonTxRef, evidence ...*request.ManualPaymentEvidence) (string, error) {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return "", fmt.Errorf("fetch current masterchain: %w", err)
	}
	txs, err := listManualTonCandidateTransactions(ctx, api, master, receive, ref)
	if err != nil {
		return "", fmt.Errorf("list ton account transactions: %w", err)
	}
	if len(txs) == 0 {
		return "", fmt.Errorf("transaction not found")
	}

	state, err := manualTonTokenState(ctx, api, master, receive)
	if err != nil {
		return "", err
	}
	return validateManualTonPaymentCandidates(order, receive, ref, txs, state, evidence...)
}

func validateManualTonPaymentCandidates(order *mdb.Orders, receive *address.Address, ref manualTonTxRef, txs []*tlb.Transaction, state *manualTonState, evidence ...*request.ManualPaymentEvidence) (string, error) {
	if state == nil {
		state = &manualTonState{jettonWallets: make(map[string]mdb.ChainToken)}
	}
	var matched []*TonObservedTransfer
	var amountErrors []error
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		hashHex := hex.EncodeToString(tx.Hash)
		if !strings.EqualFold(hashHex, ref.HashHex) {
			continue
		}
		if ref.HasLT && tx.LT != ref.LT {
			continue
		}
		transfer, err := ParseTonInboundTransfer(tx, receive, state.nativeToken, state.jettonWallets)
		if err != nil || transfer == nil {
			continue
		}
		err = EnsureTonTransferMatchesOrder(order, transfer)
		var mismatch *AmountMismatchError
		if err != nil && !errors.As(err, &mismatch) {
			return "", err
		}
		matched = append(matched, transfer)
		amountErrors = append(amountErrors, err)
	}
	if len(matched) == 0 {
		return "", fmt.Errorf("matching ton transfer to order address not found")
	}
	if len(matched) > 1 && !ref.HasLT {
		return "", fmt.Errorf("ton transaction hash matched multiple recent transactions; submit canonical id or lt:hash")
	}
	if amountErrors[0] == nil {
		captureManualEvidence(evidence, matched[0].RawAmount, matched[0].Token.Decimals, matched[0].BlockTimeMs, matched[0].Token.ContractAddress, fmt.Sprintf("inbound:%d", matched[0].LT))
	}
	if mismatch, ok := amountErrors[0].(*AmountMismatchError); ok {
		mismatch.Evidence.EventID = fmt.Sprintf("inbound:%d", matched[0].LT)
	}
	receiveRaw := addressutil.TonRawAddressObjectKey(receive)
	return TonCanonicalBlockTransactionID(receiveRaw, matched[0].LT, matched[0].TxHashHex), amountErrors[0]
}

func listManualTonCandidateTransactions(ctx context.Context, api ton.APIClientWrapped, master *ton.BlockIDExt, receive *address.Address, ref manualTonTxRef) ([]*tlb.Transaction, error) {
	waiter := api.WaitForBlock(master.SeqNo)
	if ref.HasLT {
		hashBytes, err := hex.DecodeString(ref.HashHex)
		if err != nil {
			return nil, err
		}
		return waiter.ListTransactions(ctx, receive, 1, ref.LT, hashBytes)
	}

	account, err := waiter.GetAccount(ctx, master, receive)
	if err != nil {
		return nil, fmt.Errorf("fetch ton account state: %w", err)
	}
	if account == nil || account.LastTxLT == 0 || len(account.LastTxHash) == 0 {
		return nil, fmt.Errorf("transaction not found")
	}
	return waiter.ListTransactions(ctx, receive, 100, account.LastTxLT, account.LastTxHash)
}

type manualTonState struct {
	nativeToken   *mdb.ChainToken
	jettonWallets map[string]mdb.ChainToken
}

func manualTonTokenState(ctx context.Context, api ton.APIClientWrapped, master *ton.BlockIDExt, receive *address.Address) (*manualTonState, error) {
	tokens, err := data.ListEnabledChainTokensByNetwork(mdb.NetworkTon)
	if err != nil {
		return nil, err
	}
	state := &manualTonState{jettonWallets: make(map[string]mdb.ChainToken)}
	for i := range tokens {
		sym := strings.ToUpper(strings.TrimSpace(tokens[i].Symbol))
		contract := strings.TrimSpace(tokens[i].ContractAddress)
		if sym == TonNativeSymbol && contract == "" {
			token := tokens[i]
			state.nativeToken = &token
			continue
		}
		if contract == "" {
			continue
		}
		masterAddr, err := addressutil.ParseTonMainnetAddress(contract)
		if err != nil {
			continue
		}
		wallet, err := jetton.NewJettonMasterClient(api, masterAddr.Address).GetJettonWalletAtBlock(ctx, receive, master)
		if err != nil {
			continue
		}
		state.jettonWallets[addressutil.TonRawAddressObjectKey(wallet.Address())] = tokens[i]
	}
	return state, nil
}

func validateManualSolanaPayment(order *mdb.Orders, sig string, evidence ...*request.ManualPaymentEvidence) (string, error) {
	sig = strings.TrimSpace(sig)
	nodes, err := data.ListManualPaymentRpcCandidates(mdb.NetworkSolana, mdb.RpcNodeTypeHttp)
	if err != nil {
		return "", err
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("no enabled %s %s RPC node configured in rpc_nodes", mdb.NetworkSolana, mdb.RpcNodeTypeHttp)
	}

	var verifyErrors []string
	var mismatchErr *AmountMismatchError
	for _, node := range nodes {
		rpcURL := strings.TrimSpace(node.Url)
		if rpcURL == "" {
			continue
		}
		if err = validateManualSolanaPaymentWithRPC(order, sig, node, evidence...); err != nil {
			if mismatchErr == nil {
				errors.As(err, &mismatchErr)
			}
			verifyErrors = append(verifyErrors, fmt.Sprintf("%s: %v", manualRpcNodeLabel(node), err))
			continue
		}
		return sig, nil
	}
	if mismatchErr != nil {
		return sig, mismatchErr
	}
	if len(verifyErrors) > 0 {
		return "", fmt.Errorf("manual Solana verification failed: %s", strings.Join(verifyErrors, "; "))
	}
	return "", fmt.Errorf("no enabled %s %s RPC node configured in rpc_nodes", mdb.NetworkSolana, mdb.RpcNodeTypeHttp)
}

func validateManualSolanaPaymentWithRPC(order *mdb.Orders, sig string, node mdb.RpcNode, evidence ...*request.ManualPaymentEvidence) error {
	txData, err := solGetTransactionWithNodeHeaders(node, sig, nil)
	if err != nil {
		return fmt.Errorf("fetch solana transaction: %w", err)
	}
	if !gjson.GetBytes(txData, "result").Exists() || gjson.GetBytes(txData, "result").Type == gjson.Null {
		return fmt.Errorf("transaction not found")
	}
	if err = ensureSolanaConfirmationsWithNode(order.Network, sig, node); err != nil {
		return err
	}

	tokens, err := data.ListEnabledChainTokensByNetwork(mdb.NetworkSolana)
	if err != nil {
		return err
	}
	mintTokens := make(map[string]*mdb.ChainToken, len(tokens))
	var nativeSolToken *mdb.ChainToken
	for i := range tokens {
		sym := strings.ToUpper(strings.TrimSpace(tokens[i].Symbol))
		mint := strings.TrimSpace(tokens[i].ContractAddress)
		if sym == "SOL" && mint == "" {
			nativeSolToken = &tokens[i]
			continue
		}
		if mint != "" {
			mintTokens[mint] = &tokens[i]
		}
	}

	watched := []string{order.ReceiveAddress}
	for mint := range mintTokens {
		ata, err := FindATAAddress(order.ReceiveAddress, mint)
		if err != nil {
			return err
		}
		watched = append(watched, ata)
	}
	transfers, err := validateSolTransactionTransfers(txData, sig, watched, mintTokens)
	if err != nil {
		return err
	}
	var mismatch *AmountMismatchError
	for index, transferInfo := range transfers {
		if transferInfo == nil {
			continue
		}
		if !isTransferToAddress(transferInfo, order.ReceiveAddress) {
			continue
		}
		token, _ := resolveSolTokenAndAmount(transferInfo, mintTokens, nativeSolToken)
		if !strings.EqualFold(token, order.Token) {
			continue
		}
		if err = ensureSolanaTransferNotBeforeOrder(transferInfo.BlockTime, order); err != nil {
			return err
		}
		cfg := mintTokens[transferInfo.Mint]
		asset := transferInfo.Mint
		if asset == "SOL" {
			cfg, asset = nativeSolToken, ""
		}
		if manualOrderAmountMatches(order, new(big.Int).SetUint64(transferInfo.RawAmount), cfg.Decimals) {
			captureManualEvidence(evidence, new(big.Int).SetUint64(transferInfo.RawAmount), cfg.Decimals, transferInfo.BlockTime*1000, asset, fmt.Sprintf("instruction:%d", index))
			return nil
		}
		mismatch = newAmountMismatch(new(big.Int).SetUint64(transferInfo.RawAmount), cfg.Decimals, transferInfo.BlockTime*1000, asset, fmt.Sprintf("instruction:%d", index))
	}
	if mismatch != nil {
		return mismatch
	}
	return fmt.Errorf("matching solana transfer to order address not found")
}

func ensureSolanaTransferNotBeforeOrder(blockTime int64, order *mdb.Orders) error {
	if order == nil {
		return fmt.Errorf("order not found")
	}
	if blockTime <= 0 {
		return fmt.Errorf("transaction block time missing")
	}
	if blockTime*1000 < data.PaymentAssignmentTimeMs(order) {
		return fmt.Errorf("transaction predates the order")
	}
	return nil
}

func ensureSolanaConfirmations(network, sig string) error {
	node, err := resolveSolanaRpcNode()
	if err != nil {
		return err
	}
	return ensureSolanaConfirmationsWithNode(network, sig, *node)
}

func ensureSolanaConfirmationsWithRPC(network, sig string, rpcURL string) error {
	return ensureSolanaConfirmationsWithNode(network, sig, mdb.RpcNode{Network: network, Type: mdb.RpcNodeTypeHttp, Url: rpcURL})
}

func ensureSolanaConfirmationsWithNode(network, sig string, node mdb.RpcNode) error {
	chain, err := data.GetChainByNetwork(network)
	if err != nil {
		return err
	}
	minConfirmations := 1
	if chain != nil && chain.MinConfirmations > 0 {
		minConfirmations = chain.MinConfirmations
	}
	if minConfirmations <= 1 {
		return nil
	}

	body, err := solRetryClientWithNodeHeaders(node, "getSignatureStatuses", []interface{}{
		[]string{sig},
		map[string]interface{}{"searchTransactionHistory": true},
	}, nil)
	if err != nil {
		return fmt.Errorf("fetch solana signature status: %w", err)
	}
	status := gjson.GetBytes(body, "result.value.0")
	if !status.Exists() || status.Type == gjson.Null {
		return fmt.Errorf("transaction status not found")
	}
	if errValue := status.Get("err"); errValue.Exists() && errValue.Type != gjson.Null {
		return fmt.Errorf("transaction is not successful: %s", errValue.Raw)
	}
	if status.Get("confirmationStatus").String() == "finalized" {
		return nil
	}
	confirmations := status.Get("confirmations").Int()
	if confirmations < int64(minConfirmations) {
		return fmt.Errorf("transaction confirmations %d below required %d", confirmations, minConfirmations)
	}
	return nil
}

func amountMatchesRaw(expected float64, rawAmount *big.Int, decimals int) bool {
	if rawAmount == nil || rawAmount.Sign() <= 0 {
		return false
	}
	if decimals < 0 {
		decimals = 0
	}
	actual := decimal.NewFromBigInt(rawAmount, -int32(decimals))
	return roundedAmount(expected).Equal(actual.Round(int32(data.GetAmountPrecision())))
}

func amountMatchesFloat(expected, actual float64) bool {
	return roundedAmount(expected).Equal(roundedAmount(actual))
}

func roundedAmount(amount float64) decimal.Decimal {
	precision := data.GetAmountPrecision()
	return decimal.NewFromFloat(math.MustParsePrecFloat64(amount, precision)).Round(int32(precision))
}

func rawAmountToFloat(rawAmount *big.Int, decimals int) float64 {
	if rawAmount == nil {
		return 0
	}
	if decimals < 0 {
		decimals = 0
	}
	d := decimal.NewFromBigInt(rawAmount, -int32(decimals))
	f, _ := d.Float64()
	return f
}

// AmountMismatchError is returned when a transaction's actual paid amount
// differs from the order's expected amount. It carries the actual amount so
// that callers (e.g. package EPay tolerance) can decide whether to accept.
type AmountMismatchError struct {
	ActualPaidAmount float64
	Evidence         *request.ManualPaymentEvidence
}

func newAmountMismatch(raw *big.Int, decimals int, stamp int64, asset string, eventIDs ...string) *AmountMismatchError {
	if raw == nil {
		return &AmountMismatchError{}
	}
	proof := &request.ManualPaymentEvidence{RawAmount: raw.String(), Decimals: decimals, BlockTimeMs: stamp, AssetID: asset}
	if len(eventIDs) > 0 {
		proof.EventID = eventIDs[0]
	}
	return &AmountMismatchError{ActualPaidAmount: rawAmountToFloat(raw, decimals), Evidence: proof}
}

func (e *AmountMismatchError) Error() string {
	return "transaction amount mismatch"
}

func tronHexToAddress(hexAddr string) (string, error) {
	hexAddr, err := normalizeTronAddressHex(hexAddr)
	if err != nil {
		return "", err
	}
	raw, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", err
	}
	return tron.EncodeCheck(raw), nil
}

func tronAddressToHex(addr string) (string, error) {
	raw, err := tron.DecodeCheck(strings.TrimSpace(addr))
	if err != nil {
		return "", err
	}
	if len(raw) != 21 || raw[0] != tron.PrefixMainnet {
		return "", fmt.Errorf("invalid tron address")
	}
	return strings.ToLower(hex.EncodeToString(raw)), nil
}

func normalizeTronAddressHex(hexAddr string) (string, error) {
	hexAddr = normalizeTronHexAddress(hexAddr)
	raw, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", err
	}
	switch len(raw) {
	case 20:
		raw = append([]byte{tron.PrefixMainnet}, raw...)
	case 21:
		if raw[0] != tron.PrefixMainnet {
			return "", fmt.Errorf("invalid tron address prefix")
		}
	default:
		return "", fmt.Errorf("invalid address length: %d", len(raw))
	}
	return strings.ToLower(hex.EncodeToString(raw)), nil
}

func normalizeTronHexAddress(hexAddr string) string {
	hexAddr = strings.TrimSpace(hexAddr)
	hexAddr = strings.TrimPrefix(hexAddr, "0x")
	hexAddr = strings.TrimPrefix(hexAddr, "0X")
	return strings.ToLower(hexAddr)
}

func normalizeEventTopic(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "0x")
	value = strings.TrimPrefix(value, "0X")
	return strings.ToLower(value)
}

func tronTopicAddressToHex(topic string) (string, error) {
	topic = normalizeEventTopic(topic)
	if len(topic) != 64 {
		return "", fmt.Errorf("invalid TRON event address topic length")
	}
	if _, err := hex.DecodeString(topic); err != nil {
		return "", err
	}
	return normalizeTronAddressHex(topic[24:])
}

func tronEventDataAmount(data string) (*big.Int, error) {
	data = normalizeEventTopic(data)
	if data == "" {
		return nil, fmt.Errorf("empty event data")
	}
	if len(data)%2 != 0 {
		return nil, fmt.Errorf("odd-length event data")
	}
	raw, err := hex.DecodeString(data)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(raw), nil
}

func isEvmHash(value string) bool {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "0x")
	value = strings.TrimPrefix(value, "0X")
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func canonicalEvmHash(value string) (common.Hash, string, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "0x")
	value = strings.TrimPrefix(value, "0X")
	if len(value) != 64 {
		return common.Hash{}, "", fmt.Errorf("invalid EVM transaction hash")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return common.Hash{}, "", err
	}
	hash := common.HexToHash("0x" + strings.ToLower(value))
	return hash, hash.Hex(), nil
}

func normalizeTronTxID(value string) (string, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "0x")
	value = strings.TrimPrefix(value, "0X")
	if len(value) != 64 {
		return "", fmt.Errorf("invalid TRON transaction id length")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", err
	}
	return strings.ToLower(value), nil
}

func normalizeEvmAddress(value string) (common.Address, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "0x")
	value = strings.TrimPrefix(value, "0X")
	if len(value) != 40 {
		return common.Address{}, fmt.Errorf("invalid address length")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return common.Address{}, err
	}
	return common.HexToAddress("0x" + value), nil
}

// Capture only an accepted exact transfer. Failed node attempts must not provide
// settlement evidence; amount-mismatch errors carry their own selected evidence.
func captureManualEvidence(sinks []*request.ManualPaymentEvidence, raw *big.Int, decimals int, stamp int64, asset string, eventIDs ...string) {
	if len(sinks) > 0 && sinks[0] != nil && raw != nil {
		*sinks[0] = request.ManualPaymentEvidence{RawAmount: raw.String(), Decimals: decimals, BlockTimeMs: stamp, AssetID: asset}
		if len(eventIDs) > 0 {
			sinks[0].EventID = eventIDs[0]
		}
	}
}

// New orders keep the amount precision used when their address was reserved.
// An administrator changing the global precision must not change an old quote.
func manualOrderAmountMatches(order *mdb.Orders, raw *big.Int, decimals int) bool {
	if order.AmountPrecision == nil {
		return amountMatchesRaw(order.ActualAmount, raw, decimals)
	}
	if raw == nil || raw.Sign() <= 0 || decimals < 0 || decimals > 36 {
		return false
	}
	precision := int32(data.NormalizeAmountPrecision(*order.AmountPrecision))
	return decimal.NewFromFloat(order.ActualAmount).Round(precision).Equal(decimal.NewFromBigInt(raw, -int32(decimals)).Round(precision))
}
