package task

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	tron "github.com/GMWalletApp/epusdt/crypto"
	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/tidwall/gjson"
)

const (
	// transfer(address,uint256) 方法签名前4字节
	TransferMethodID = "a9059cbb"

	PollInterval   = 3 * time.Second
	RequestTimeout = 10 * time.Second
)

func HexToTronAddress(hexAddr string) (string, error) {
	hexAddr = strings.TrimPrefix(hexAddr, "0x")
	hexAddr = strings.TrimPrefix(hexAddr, "0X")

	raw, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", err
	}

	// 确保有 0x41 前缀（TRON 主网地址前缀）
	if len(raw) == 20 {
		raw = append([]byte{0x41}, raw...)
	}
	if len(raw) != 21 {
		return "", fmt.Errorf("地址长度非法: %d bytes", len(raw))
	}

	return tron.EncodeCheck(raw), nil
}

func TronAddressToHex(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", fmt.Errorf("地址为空")
	}
	raw, err := tron.DecodeCheck(addr)
	if err != nil {
		return "", err
	}
	if len(raw) != 21 {
		return "", fmt.Errorf("地址长度非法: %d bytes", len(raw))
	}
	if raw[0] != 0x41 {
		return "", fmt.Errorf("非法 TRON 主网地址前缀: 0x%x", raw[0])
	}
	return strings.ToLower(hex.EncodeToString(raw)), nil
}

func normalizeTronHexAddress(hexAddr string) string {
	hexAddr = strings.TrimSpace(hexAddr)
	hexAddr = strings.TrimPrefix(hexAddr, "0x")
	hexAddr = strings.TrimPrefix(hexAddr, "0X")
	return strings.ToLower(hexAddr)
}

type BlockHeader struct {
	RawData struct {
		Number         int64  `json:"number"`
		Timestamp      int64  `json:"timestamp"`
		WitnessAddress string `json:"witness_address"`
		ParentHash     string `json:"parentHash"`
		Version        int    `json:"version"`
	} `json:"raw_data"`
}

type TriggerSmartContractValue struct {
	OwnerAddress    string `json:"owner_address"`
	ContractAddress string `json:"contract_address"`
	Data            string `json:"data"`
	CallValue       int64  `json:"call_value"`
}

type ContractParam struct {
	TypeURL string          `json:"type_url"`
	Value   json.RawMessage `json:"value"`
}

type Transaction struct {
	TxID    string `json:"txID"`
	RawData struct {
		Contract []struct {
			Type      string        `json:"type"`
			Parameter ContractParam `json:"parameter"`
		} `json:"contract"`
		Timestamp int64 `json:"timestamp"`
		FeeLimit  int64 `json:"fee_limit"`
	} `json:"raw_data"`
	Ret []struct {
		ContractRet string `json:"contractRet"`
	} `json:"ret"`
}

type Block struct {
	BlockID      string        `json:"blockID"`
	BlockHeader  BlockHeader   `json:"block_header"`
	Transactions []Transaction `json:"transactions"`
}

type TRC20Transfer struct {
	TxID   string
	From   string
	To     string
	Raw    *big.Int
	Status string
	Token  mdb.ChainToken
}

type TRXTransfer struct {
	TxID   string
	From   string
	To     string
	RawSun int64 // 单位: SUN
	Status string
}

type TransferContractValue struct {
	OwnerAddress string `json:"owner_address"`
	ToAddress    string `json:"to_address"`
	Amount       int64  `json:"amount"` // 单位: SUN
}

var httpClient = &http.Client{Timeout: RequestTimeout}

func doPost(url string, apiKey string, body interface{}) ([]byte, error) {
	raw, _, err := doPostNode(mdb.RpcNode{Network: mdb.NetworkTron, Type: mdb.RpcNodeTypeHttp, Url: url, ApiKey: apiKey}, url, body, "")
	return raw, err
}

func doPostNode(node mdb.RpcNode, url string, body interface{}, reason string) ([]byte, time.Time, error) {
	data, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		return nil, time.Time{}, marshalErr
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if apiKey := strings.TrimSpace(node.ApiKey); apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", apiKey)
	}

	client := *httpClient
	client.Timeout = service.RPCRequestTimeout(node, client.Timeout)
	client.Transport = service.NewRPCTransport(node, reason, httpClient.Transport)
	resp, err := client.Do(req)
	if err != nil {
		return nil, time.Time{}, service.WrapRPCError(err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, time.Time{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, service.RPCStatusError(resp.StatusCode)
	}
	return b, service.RPCRequestStartedAt(resp), nil
}

func GetNowBlock(baseURL string, apiKey string) (*Block, error) {
	b, err := doPost(baseURL+"/wallet/getnowblock", apiKey, map[string]interface{}{})
	if err != nil {
		data.RecordRpcFailure(mdb.NetworkTron)
		return nil, err
	}
	var block Block
	if err := json.Unmarshal(b, &block); err != nil {
		data.RecordRpcFailure(mdb.NetworkTron)
		return nil, err
	}
	data.RecordRpcSuccess(mdb.NetworkTron)
	data.RecordRpcBlockHeight(mdb.NetworkTron, block.BlockHeader.RawData.Number)
	return &block, nil
}

func GetBlockByNum(baseURL string, apiKey string, num int64) (*Block, error) {
	b, err := doPost(baseURL+"/wallet/getblockbynum", apiKey, map[string]interface{}{"num": num})
	if err != nil {
		data.RecordRpcFailure(mdb.NetworkTron)
		return nil, err
	}
	var block Block
	if err := json.Unmarshal(b, &block); err != nil {
		data.RecordRpcFailure(mdb.NetworkTron)
		return nil, err
	}
	data.RecordRpcSuccess(mdb.NetworkTron)
	return &block, nil
}

func loadTronTRC20TokenMap() (map[string]mdb.ChainToken, error) {
	tokens, err := data.ListEnabledChainTokensByNetwork(mdb.NetworkTron)
	if err != nil {
		return nil, err
	}
	tokenMap := make(map[string]mdb.ChainToken)
	for _, token := range tokens {
		symbol := strings.ToUpper(strings.TrimSpace(token.Symbol))
		contractAddress := strings.TrimSpace(token.ContractAddress)
		if symbol == "TRX" || contractAddress == "" {
			continue
		}
		contractHex, err := TronAddressToHex(contractAddress)
		if err != nil {
			log.Sugar.Warnf("[TRON-BLOCK] skip invalid TRC20 contract symbol=%s address=%s err=%v", symbol, contractAddress, err)
			continue
		}
		tokenMap[contractHex] = token
	}
	return tokenMap, nil
}

func parseTRC20Transfer(tx Transaction, tokenMap map[string]mdb.ChainToken) *TRC20Transfer {
	if len(tokenMap) == 0 {
		return nil
	}
	if len(tx.RawData.Contract) == 0 {
		return nil
	}
	c := tx.RawData.Contract[0]
	if c.Type != "TriggerSmartContract" {
		return nil
	}

	if !gjson.ValidBytes(c.Parameter.Value) {
		return nil
	}
	value := gjson.ParseBytes(c.Parameter.Value)
	val := TriggerSmartContractValue{OwnerAddress: value.Get("owner_address").String(), ContractAddress: value.Get("contract_address").String(), Data: value.Get("data").String()}
	for _, field := range []string{"owner_address", "contract_address", "data"} {
		if value.Get(field).Type != gjson.String {
			return nil
		}
	}

	contractHex := normalizeTronHexAddress(val.ContractAddress)
	token, ok := tokenMap[contractHex]
	if !ok {
		return nil
	}

	// 解析 data 字段
	// 格式: [4字节方法ID][32字节 to 地址][32字节 amount]
	data := strings.TrimPrefix(strings.ToLower(val.Data), "0x")
	if len(data) < 8+64+64 {
		return nil
	}
	if data[:8] != TransferMethodID {
		return nil
	}

	// to 地址：后 40 个十六进制字符（20 字节）
	toHex := data[8+24 : 8+64] // 跳过前12字节填充，取后20字节
	toAddr, err := HexToTronAddress(toHex)
	if err != nil {
		return nil
	}

	// amount：后 32 字节大整数
	amountHex := data[8+64 : 8+64+64]
	amountBytes, err := hex.DecodeString(amountHex)
	if err != nil {
		return nil
	}
	amountBig := new(big.Int).SetBytes(amountBytes)

	// from 地址
	fromAddr, err := HexToTronAddress(val.OwnerAddress)
	if err != nil {
		fromAddr = val.OwnerAddress
	}

	// 交易状态
	status := "SUCCESS"
	if len(tx.Ret) > 0 && tx.Ret[0].ContractRet != "" && tx.Ret[0].ContractRet != "SUCCESS" {
		status = tx.Ret[0].ContractRet
	}

	return &TRC20Transfer{
		TxID:   tx.TxID,
		From:   fromAddr,
		To:     toAddr,
		Raw:    amountBig,
		Status: status,
		Token:  token,
	}
}

func parseTRXTransfer(tx Transaction) *TRXTransfer {
	if len(tx.RawData.Contract) == 0 {
		return nil
	}
	c := tx.RawData.Contract[0]
	if c.Type != "TransferContract" {
		return nil
	}

	if !gjson.ValidBytes(c.Parameter.Value) {
		return nil
	}
	value := gjson.ParseBytes(c.Parameter.Value)
	if value.Get("owner_address").Type != gjson.String || value.Get("to_address").Type != gjson.String {
		return nil
	}
	amount, err := strconv.ParseInt(value.Get("amount").Raw, 10, 64)
	if err != nil {
		return nil
	}
	val := TransferContractValue{OwnerAddress: value.Get("owner_address").String(), ToAddress: value.Get("to_address").String(), Amount: amount}
	if val.Amount <= 0 {
		return nil
	}

	fromAddr, err := HexToTronAddress(val.OwnerAddress)
	if err != nil {
		fromAddr = val.OwnerAddress
	}
	toAddr, err := HexToTronAddress(val.ToAddress)
	if err != nil {
		toAddr = val.ToAddress
	}

	status := "SUCCESS"
	if len(tx.Ret) > 0 && tx.Ret[0].ContractRet != "" && tx.Ret[0].ContractRet != "SUCCESS" {
		status = tx.Ret[0].ContractRet
	}

	return &TRXTransfer{
		TxID:   tx.TxID,
		From:   fromAddr,
		To:     toAddr,
		RawSun: val.Amount,
		Status: status,
	}
}

func processBlock(block *Block, tokenMap map[string]mdb.ChainToken) error {
	blockTsMs := block.BlockHeader.RawData.Timestamp
	for _, tx := range block.Transactions {
		if t := parseTRC20Transfer(tx, tokenMap); t != nil {
			if t.Status != "SUCCESS" {
				continue
			}
			if err := service.ProcessTronTRC20Transfer(t.Token, t.To, t.Raw, t.TxID, blockTsMs); err != nil {
				return err
			}
			continue
		}
		if t := parseTRXTransfer(tx); t != nil {
			if t.Status != "SUCCESS" {
				continue
			}
			if err := service.ProcessTronTRXTransfer(t.To, t.RawSun, t.TxID, blockTsMs); err != nil {
				return err
			}
		}
	}
	return nil
}

type Scanner struct {
	baseURL   string
	apiKey    string
	nodeID    uint64
	nodeLabel string
	lastBlock int64
	// 统计
	totalBlocks   int64
	totalTRC20Txs int64
	totalTRXTxs   int64
}

func NewScanner() *Scanner {
	return &Scanner{}
}

func (s *Scanner) Init() error {
	node, err := service.ResolveTronRpcNode()
	if err != nil {
		return fmt.Errorf("resolve tron node: %w", err)
	}
	s.useRpcNode(node)

	log.Sugar.Infof("[TRON-BLOCK] using RPC node %s", s.nodeLabel)
	block, err := s.fetchBlock(nil)
	if err != nil {

		return fmt.Errorf("获取初始块失败: %w", err)
	}
	s.lastBlock = block.BlockHeader.RawData.Number
	saved, err := data.LoadPaymentCursor("tron:blocks")
	if err != nil {
		return err
	}
	if saved != "" {
		s.lastBlock, err = strconv.ParseInt(saved, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid tron cursor: %w", err)
		}
	} else {
		// Start behind the confirmation window so initialization cannot skip
		// newly assigned payments awaiting confirmation.
		if chain, err := data.GetChainByNetwork(mdb.NetworkTron); err != nil {
			return err
		} else if chain != nil {
			s.lastBlock -= int64(chain.MinConfirmations)
		}
		if s.lastBlock < 0 {
			s.lastBlock = 0
		}
		if err := data.SavePaymentCursor("tron:blocks", strconv.FormatInt(s.lastBlock, 10)); err != nil {
			return err
		}
	}
	log.Sugar.Infof("[TRON-BLOCK] start block=%d", s.lastBlock)
	return nil
}

func (s *Scanner) useRpcNode(node *mdb.RpcNode) {
	s.baseURL = strings.TrimRight(strings.TrimSpace(node.Url), "/")
	s.apiKey = node.ApiKey
	s.nodeID = node.ID
	s.nodeLabel = data.RpcNodeLogLabel(*node)
}

func (s *Scanner) Run() {
	log.Sugar.Info("[TRON-BLOCK] start scanning (TRC20 + TRX)")
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()

	statTicker := time.NewTicker(60 * time.Second)
	defer statTicker.Stop()

	for {
		select {
		case <-statTicker.C:
			log.Sugar.Infof("[TRON-BLOCK] stats blocks=%d trc20=%d trx=%d", s.totalBlocks, s.totalTRC20Txs, s.totalTRXTxs)
		case <-ticker.C:
			s.poll()
		}
	}
}

func (s *Scanner) poll() {
	var scanErr error = fmt.Errorf("TRON scan incomplete")
	defer func() { data.RecordRpcScanResult(mdb.NetworkTron, scanErr) }()
	latest, err := s.fetchBlock(nil)
	if err != nil {
		log.Sugar.Warnf("[TRON-BLOCK] get latest block: %v", err)

		return
	}
	latestNum := latest.BlockHeader.RawData.Number
	chain, err := data.GetChainByNetwork(mdb.NetworkTron)
	if err != nil {
		log.Sugar.Errorf("[TRON-BLOCK] load chain: %v", err)
		return
	}
	if chain != nil && chain.MinConfirmations > 1 {
		latestNum -= int64(chain.MinConfirmations - 1)
	}
	if latestNum <= s.lastBlock {
		data.RecordRpcNodeSuccess(s.nodeID)
		scanErr = nil
		return
	}

	tokenMap, err := loadTronTRC20TokenMap()
	if err != nil {
		log.Sugar.Errorf("[TRON-BLOCK] tokens: %v", err)
		return
	}
	hadBlockFetchError := false
	for num := s.lastBlock + 1; num <= latestNum; num++ {
		var block *Block
		if num == latest.BlockHeader.RawData.Number {
			block = latest
		} else {
			block, err = s.fetchBlock(&num)
			if err != nil {
				log.Sugar.Warnf("[TRON-BLOCK] get block %d: %v", num, err)
				hadBlockFetchError = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if block.BlockHeader.RawData.Number != num || block.BlockHeader.RawData.Timestamp <= 0 {
			log.Sugar.Errorf("[TRON-BLOCK] invalid block response for %d", num)
			return
		}
		if err := processBlock(block, tokenMap); err != nil {
			log.Sugar.Errorf("[TRON-BLOCK] process %d: %v", num, err)
			return
		}
		if err := data.SavePaymentCursor("tron:blocks", strconv.FormatInt(num, 10)); err != nil {
			log.Sugar.Errorf("[TRON-BLOCK] save cursor: %v", err)
			return
		}
		s.lastBlock = num
		s.totalBlocks++

		for _, tx := range block.Transactions {
			if parseTRC20Transfer(tx, tokenMap) != nil {
				s.totalTRC20Txs++
			} else if parseTRXTransfer(tx) != nil {
				s.totalTRXTxs++
			}
		}
	}
	if hadBlockFetchError {

		return
	}
	data.RecordRpcNodeSuccess(s.nodeID)
	scanErr = nil
}

// Resolve on every read so recovery returns traffic from a paid node to public
// nodes. All attempts fetch the same block; only poll() commits the cursor.
func (s *Scanner) fetchBlock(number *int64) (*Block, error) {
	tried := []uint64{}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		node, err := service.ResolveTronRpcNode(tried...)
		if err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
		s.useRpcNode(node)
		path, body := "/wallet/getnowblock", map[string]interface{}{}
		if number != nil {
			path = "/wallet/getblockbynum"
			body["num"] = *number
		}
		raw, requestStarted, err := doPostNode(*node, s.baseURL+path, body, service.RPCErrorReason(lastErr))
		var block Block
		if err == nil {
			err = validateTronBlockResponse(raw, number)
			if err == nil {
				block = decodeValidatedTronBlock(raw)
			}
			if err == nil && number == nil && !data.ObserveRpcHead(*node, block.BlockHeader.RawData.Number, requestStarted) {
				err = &service.RPCRequestError{Reason: "stale_head"}
			}
		}
		if err == nil {
			data.RecordRpcNodeSuccess(node.ID)
			data.RecordRpcSuccess(mdb.NetworkTron)
			if number == nil {
				data.RecordRpcBlockHeight(mdb.NetworkTron, block.BlockHeader.RawData.Number)
			}
			return &block, nil
		}
		lastErr = err
		data.RecordRpcFailure(mdb.NetworkTron)
		if service.RPCErrorReason(err) == "local_capacity" {
			tried = append(tried, node.ID)
			continue
		}
		data.RecordRpcNodeFailure(node.ID)
		if service.RPCErrorReason(err) != "rate_limited" {
			data.CooldownRpcEndpoint(*node, "")
		}
		log.Sugar.Warnf("[TRON-BLOCK] RPC failed node=%s reason=%s; trying alternate", data.RpcNodeLogLabel(*node), service.RPCErrorReason(err))
		tried = append(tried, node.ID)
	}
	return nil, lastErr
}

func StartTronBlockScannerListener() {
	for {
		scanner := NewScanner()
		if err := scanner.Init(); err != nil {
			log.Sugar.Errorf("[TRON-BLOCK] init: %v, retrying...", err)
			time.Sleep(10 * time.Second)
			continue
		}
		scanner.Run()
		log.Sugar.Warn("[TRON-BLOCK] scanner stopped, restarting...")
		time.Sleep(3 * time.Second)
	}
}
