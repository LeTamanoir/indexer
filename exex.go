package exex

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Handler applies committed chain segments and reverts old chain segments.
type Handler interface {
	Commit(ctx context.Context, chain Chain) error
	Revert(ctx context.Context, chain Chain) error
}

// NotificationKind identifies the kind of chain update.
type NotificationKind uint8

const (
	ChainCommitted NotificationKind = iota + 1
	ChainReorged
	ChainReverted
)

func (k NotificationKind) String() string {
	switch k {
	case ChainCommitted:
		return "committed"
	case ChainReorged:
		return "reorged"
	case ChainReverted:
		return "reverted"
	default:
		return "unknown"
	}
}

// Notification describes a chain transition.
//
// ChainCommitted carries only the new chain, ChainReverted carries only the
// old chain, and ChainReorged carries both.
type Notification struct {
	Kind NotificationKind
	Old  Chain
	New  Chain
}

// NewChainCommitted creates a notification for a chain extension.
func NewChainCommitted(chain Chain) Notification {
	return Notification{Kind: ChainCommitted, New: chain}
}

// NewChainReorged creates a notification for a chain reorganization.
func NewChainReorged(oldChain, newChain Chain) Notification {
	return Notification{Kind: ChainReorged, Old: oldChain, New: newChain}
}

// NewChainReverted creates a notification for a chain rollback.
func NewChainReverted(chain Chain) Notification {
	return Notification{Kind: ChainReverted, Old: chain}
}

// CommittedChain returns the new chain from committed and reorged
// notifications.
func (n Notification) CommittedChain() (Chain, bool) {
	switch n.Kind {
	case ChainCommitted, ChainReorged:
		return n.New, true
	default:
		return Chain{}, false
	}
}

// RevertedChain returns the old chain from reverted and reorged notifications.
func (n Notification) RevertedChain() (Chain, bool) {
	switch n.Kind {
	case ChainReverted, ChainReorged:
		return n.Old, true
	default:
		return Chain{}, false
	}
}

// Apply dispatches reverted and committed chain segments to handler.
func (n Notification) Apply(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("exex: nil handler")
	}

	if reverted, ok := n.RevertedChain(); ok {
		if err := handler.Revert(ctx, reverted); err != nil {
			return err
		}
	}

	if committed, ok := n.CommittedChain(); ok {
		if err := handler.Commit(ctx, committed); err != nil {
			return err
		}
	}

	return nil
}

// LogCount returns the number of logs carried by the notification.
func (n Notification) LogCount() int {
	count := 0
	if reverted, ok := n.RevertedChain(); ok {
		count += reverted.LogCount()
	}
	if committed, ok := n.CommittedChain(); ok {
		count += committed.LogCount()
	}
	return count
}

// Chain is the portion of chain covered by an update. Blocks contains only
// blocks that have matching logs; FromBlock and ToBlock still describe the full
// processed range, including empty blocks.
type Chain struct {
	FromBlock uint64
	ToBlock   uint64
	Blocks    []BlockLogs
}

// ForEachLog calls fn for each log in the chain.
func (c Chain) ForEachLog(fn func(log types.Log) error) error {
	if fn == nil {
		return errors.New("exex: nil log handler")
	}

	for _, block := range c.Blocks {
		for _, log := range block.Logs {
			if err := fn(log); err != nil {
				return err
			}
		}
	}
	return nil
}

// LogCount returns the number of logs carried by the chain.
func (c Chain) LogCount() int {
	count := 0
	for _, block := range c.Blocks {
		count += len(block.Logs)
	}
	return count
}

// BlockLogs contains the logs for one block.
type BlockLogs struct {
	Number uint64
	Hash   common.Hash
	Logs   []types.Log
}
