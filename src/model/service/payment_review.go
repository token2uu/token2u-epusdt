package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/GMWalletApp/epusdt/model/data"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

type paidPaymentReviewKey struct{}

// Routing is request-scoped: reviewing one order cannot move ordinary scans
// to paid nodes. Reviews intentionally require independent paid evidence.
func WithPaidPaymentReview(ctx context.Context) context.Context {
	return context.WithValue(ctx, paidPaymentReviewKey{}, true)
}

// WithoutPaidPaymentReview restores ordinary routing while retaining the
// original deadline and cancellation when paid verification is unavailable.
func WithoutPaidPaymentReview(ctx context.Context) context.Context {
	return context.WithValue(ctx, paidPaymentReviewKey{}, false)
}

func IsPaidPaymentReview(ctx context.Context) bool {
	review, _ := ctx.Value(paidPaymentReviewKey{}).(bool)
	return review
}

func SelectRPCNode(ctx context.Context, network, nodeType string, excluded ...uint64) (*mdb.RpcNode, error) {
	if !IsPaidPaymentReview(ctx) {
		if nodeType == mdb.RpcNodeTypeHttp {
			switch network {
			case mdb.NetworkSolana:
				return resolveSolanaRpcNode(excluded...)
			case mdb.NetworkAptos:
				return resolveAptosRpcNode(excluded...)
			case mdb.NetworkTron:
				return ResolveTronRpcNode(excluded...)
			}
		}
		return data.SelectGeneralRpcNode(network, nodeType, excluded...)
	}
	nodes, err := data.ListGeneralRpcCandidates(network, nodeType)
	if err != nil {
		return nil, err
	}
	for i := range nodes {
		node := &nodes[i]
		if node.Purpose != mdb.RpcNodePurposePaidFallback || data.RpcEndpointCoolingDown(*node) {
			continue
		}
		used := false
		for _, id := range excluded {
			used = used || id == node.ID
		}
		if !used {
			node.Url = strings.TrimSpace(node.Url)
			if node.Url == "" {
				return nil, fmt.Errorf("paid payment review node id=%d has empty url", node.ID)
			}
			return node, nil
		}
	}
	return nil, fmt.Errorf("no available paid payment review node network=%s type=%s", network, nodeType)
}
