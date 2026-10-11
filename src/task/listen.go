package task

import (
	"fmt"

	"github.com/GMWalletApp/epusdt/config"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/robfig/cron/v3"
)

func Start() {
	log.Sugar.Info("[task] Starting task scheduler...")
	// The ETH listener short-circuits internally when chain is disabled
	// or no tokens are configured, so always launch the goroutine.
	go StartEthereumWebSocketListener()
	go StartBscWebSocketListener()
	go StartPolygonWebSocketListener()
	go StartPlasmaWebSocketListener()
	go StartEthereumBackfillScannerListener()
	go StartBscBackfillScannerListener()
	go StartPolygonBackfillScannerListener()
	go StartPlasmaBackfillScannerListener()
	go StartTronBlockScannerListener()
	go StartTonBlockScannerListener()
	go StartAptosLedgerScannerListener()

	c := cron.New()

	// Solana polling
	solanaSchedule := fmt.Sprintf("@every %s", config.GetSolanaScanInterval())
	_, err := c.AddJob(solanaSchedule, ListenSolJob{})
	if err != nil {
		log.Sugar.Errorf("[task] Failed to add ListenSolJob: %v", err)
		return
	}

	log.Sugar.Infof("[task] ListenSolJob scheduled successfully (%s)", solanaSchedule)

	// RPC node health checks
	_, err = c.AddJob("@every 30s", RpcHealthJob{})
	if err != nil {
		log.Sugar.Errorf("[task] Failed to add RpcHealthJob: %v", err)
		return
	}
	log.Sugar.Info("[task] RpcHealthJob scheduled successfully (@every 30s)")

	c.Start()
	log.Sugar.Info("[task] Task scheduler started")
}
