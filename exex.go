package exex

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
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

func (k ExExNotificationKind) String() string {
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

// ExExNotification describes a canonical-chain transition.
//
// ChainCommitted carries only the new chain, ChainReverted carries only the
// old chain, and ChainReorged carries both.
type ExExNotification struct {
	Kind ExExNotificationKind
	Old  Chain
	New  Chain
}

// ChainHandler applies committed chain segments and reverts old chain segments.
type ChainHandler interface {
	CommitChain(ctx context.Context, chain Chain) error
	RevertChain(ctx context.Context, chain Chain) error
}

// NewExExHandler adapts a ChainHandler into an ExExHandler.
func NewExExHandler(handler ChainHandler) ExExHandler {
	return chainHandlerAdapter{handler: handler}
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

// Apply dispatches reverted and committed chain segments to handler.
func (n ExExNotification) Apply(ctx context.Context, handler ChainHandler) error {
	if handler == nil {
		return errors.New("exex: nil chain handler")
	}

	if reverted, ok := n.RevertedChain(); ok {
		if err := handler.RevertChain(ctx, reverted); err != nil {
			return err
		}
	}

	if committed, ok := n.CommittedChain(); ok {
		if err := handler.CommitChain(ctx, committed); err != nil {
			return err
		}
	}

	return nil
}

// LogCount returns the number of logs carried by the notification.
func (n ExExNotification) LogCount() int {
	count := 0
	if reverted, ok := n.RevertedChain(); ok {
		count += reverted.LogCount()
	}
	if committed, ok := n.CommittedChain(); ok {
		count += committed.LogCount()
	}
	return count
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

// ForEachLog calls fn for each log in the chain.
func (c Chain) ForEachLog(ctx context.Context, fn func(block BlockLogs, log types.Log) error) error {
	if fn == nil {
		return errors.New("exex: nil log handler")
	}

	for _, block := range c.Blocks {
		for _, log := range block.Logs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(block, log); err != nil {
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

type chainHandlerAdapter struct {
	handler ChainHandler
}

func (h chainHandlerAdapter) HandleNotification(ctx context.Context, notification ExExNotification) error {
	return notification.Apply(ctx, h.handler)
}
