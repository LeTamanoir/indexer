package exex

import (
	"context"

	"github.com/ethereum/go-ethereum/core/types"
)

// ExExHandler receives canonical-chain notifications in the style of a Reth
// execution extension.
type ExExHandler interface {
	HandleNotification(ctx context.Context, notification ExExNotification) error
}

// ExExNotificationKind identifies the kind of canonical-chain update.
type ExExNotificationKind uint8

const (
	ChainCommitted ExExNotificationKind = iota + 1
	ChainReorged
	ChainReverted
)

// ExExNotification describes a canonical-chain transition.
//
// ChainCommitted carries only the new chain, ChainReverted carries only the
// old chain, and ChainReorged carries both.
type ExExNotification struct {
	Kind ExExNotificationKind
	Old  Chain
	New  Chain
}

// NewChainCommitted creates a notification for a canonical chain extension.
func NewChainCommitted(chain Chain) ExExNotification {
	return ExExNotification{Kind: ChainCommitted, New: chain}
}

// NewChainReorged creates a notification for a chain reorganization.
func NewChainReorged(oldChain, newChain Chain) ExExNotification {
	return ExExNotification{Kind: ChainReorged, Old: oldChain, New: newChain}
}

// NewChainReverted creates a notification for a canonical chain rollback.
func NewChainReverted(chain Chain) ExExNotification {
	return ExExNotification{Kind: ChainReverted, Old: chain}
}

// CommittedChain returns the new chain from committed and reorged
// notifications.
func (n ExExNotification) CommittedChain() (Chain, bool) {
	switch n.Kind {
	case ChainCommitted, ChainReorged:
		return n.New, true
	default:
		return Chain{}, false
	}
}

// RevertedChain returns the old chain from reverted and reorged notifications.
func (n ExExNotification) RevertedChain() (Chain, bool) {
	switch n.Kind {
	case ChainReverted, ChainReorged:
		return n.Old, true
	default:
		return Chain{}, false
	}
}

// Inverted returns the opposite transition.
func (n ExExNotification) Inverted() ExExNotification {
	switch n.Kind {
	case ChainCommitted:
		return NewChainReverted(n.New)
	case ChainReverted:
		return NewChainCommitted(n.Old)
	case ChainReorged:
		return NewChainReorged(n.New, n.Old)
	default:
		return n
	}
}

// Chain is the portion of chain covered by a notification. Blocks contains only
// blocks that have matching logs; FromBlock and ToBlock still describe the full
// processed range, including empty blocks.
type Chain struct {
	FromBlock uint64
	ToBlock   uint64
	Blocks    []BlockLogs
}

// Empty reports whether the chain covers no block range.
func (c Chain) Empty() bool {
	return c.ToBlock < c.FromBlock
}

// Tip returns the highest block covered by the chain.
func (c Chain) Tip() uint64 {
	if c.Empty() {
		return 0
	}
	return c.ToBlock
}

// BlockLogs contains the logs for one block.
type BlockLogs struct {
	Number uint64
	Logs   []types.Log
}
