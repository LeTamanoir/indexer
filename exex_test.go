package exex

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestExExNotificationChains(t *testing.T) {
	oldChain := Chain{FromBlock: 10, ToBlock: 11}
	newChain := Chain{FromBlock: 10, ToBlock: 12}
	notification := NewChainReorged(oldChain, newChain)

	committed, ok := notification.CommittedChain()
	if !ok || !reflect.DeepEqual(committed, newChain) {
		t.Fatalf("committed chain = %+v, %v; want %+v, true", committed, ok, newChain)
	}

	reverted, ok := notification.RevertedChain()
	if !ok || !reflect.DeepEqual(reverted, oldChain) {
		t.Fatalf("reverted chain = %+v, %v; want %+v, true", reverted, ok, oldChain)
	}

	inverted := notification.Inverted()
	if inverted.Kind != ChainReorged || !reflect.DeepEqual(inverted.Old, newChain) || !reflect.DeepEqual(inverted.New, oldChain) {
		t.Fatalf("inverted = %+v, want old/new swapped reorg", inverted)
	}
}

func TestExExNotificationKindString(t *testing.T) {
	tests := []struct {
		kind ExExNotificationKind
		want string
	}{
		{ChainCommitted, "committed"},
		{ChainReorged, "reorged"},
		{ChainReverted, "reverted"},
		{ExExNotificationKind(255), "unknown"},
	}

	for _, test := range tests {
		if got := test.kind.String(); got != test.want {
			t.Fatalf("%v.String() = %q, want %q", uint8(test.kind), got, test.want)
		}
	}
}

func TestExExNotificationApply(t *testing.T) {
	oldChain := Chain{FromBlock: 10, ToBlock: 10}
	newChain := Chain{FromBlock: 11, ToBlock: 11}
	handler := &recordingChainHandler{}

	notification := NewChainReorged(oldChain, newChain)
	if err := notification.Apply(context.Background(), handler); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	want := []string{"revert:10-10", "commit:11-11"}
	if !reflect.DeepEqual(handler.calls, want) {
		t.Fatalf("handler calls = %v, want %v", handler.calls, want)
	}
}

func TestNewExExHandler(t *testing.T) {
	handler := &recordingChainHandler{}
	adapted := NewExExHandler(handler)

	notification := NewChainCommitted(Chain{FromBlock: 12, ToBlock: 13})
	if err := adapted.HandleNotification(context.Background(), notification); err != nil {
		t.Fatalf("HandleNotification() error = %v", err)
	}

	want := []string{"commit:12-13"}
	if !reflect.DeepEqual(handler.calls, want) {
		t.Fatalf("handler calls = %v, want %v", handler.calls, want)
	}
}

func TestExExNotificationLogCount(t *testing.T) {
	oldChain := Chain{
		FromBlock: 1,
		ToBlock:   1,
		Blocks: []BlockLogs{
			{Number: 1, Logs: []types.Log{{Index: 0}}},
		},
	}
	newChain := Chain{
		FromBlock: 2,
		ToBlock:   3,
		Blocks: []BlockLogs{
			{Number: 2, Logs: []types.Log{{Index: 0}, {Index: 1}}},
			{Number: 3, Logs: []types.Log{{Index: 0}}},
		},
	}

	notification := NewChainReorged(oldChain, newChain)
	if got := notification.LogCount(); got != 4 {
		t.Fatalf("LogCount() = %d, want 4", got)
	}
}

func TestChainHelpers(t *testing.T) {
	empty := Chain{FromBlock: 12, ToBlock: 11}
	if !empty.Empty() || empty.Tip() != 0 {
		t.Fatalf("empty chain Empty=%v Tip=%d, want true and 0", empty.Empty(), empty.Tip())
	}

	chain := Chain{FromBlock: 10, ToBlock: 12}
	if chain.Empty() || chain.Tip() != 12 {
		t.Fatalf("chain Empty=%v Tip=%d, want false and 12", chain.Empty(), chain.Tip())
	}
}

func TestChainForEachLog(t *testing.T) {
	blockHash := common.BytesToHash([]byte("block"))
	chain := Chain{
		FromBlock: 10,
		ToBlock:   11,
		Blocks: []BlockLogs{
			{
				Number: 10,
				Hash:   blockHash,
				Logs:   []types.Log{{Index: 0}, {Index: 1}},
			},
			{
				Number: 11,
				Hash:   common.BytesToHash([]byte("next")),
				Logs:   []types.Log{{Index: 2}},
			},
		},
	}

	var visited []uint
	err := chain.ForEachLog(context.Background(), func(block BlockLogs, log types.Log) error {
		if len(visited) == 0 && block.Hash != blockHash {
			t.Fatalf("first block hash = %s, want %s", block.Hash, blockHash)
		}
		visited = append(visited, log.Index)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachLog() error = %v", err)
	}

	if !reflect.DeepEqual(visited, []uint{0, 1, 2}) {
		t.Fatalf("visited logs = %v, want [0 1 2]", visited)
	}
	if got := chain.LogCount(); got != 3 {
		t.Fatalf("LogCount() = %d, want 3", got)
	}
}

type recordingChainHandler struct {
	calls []string
}

func (h *recordingChainHandler) CommitChain(ctx context.Context, chain Chain) error {
	h.calls = append(h.calls, fmt.Sprintf("commit:%d-%d", chain.FromBlock, chain.ToBlock))
	return nil
}

func (h *recordingChainHandler) RevertChain(ctx context.Context, chain Chain) error {
	h.calls = append(h.calls, fmt.Sprintf("revert:%d-%d", chain.FromBlock, chain.ToBlock))
	return nil
}
