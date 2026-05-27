package exex

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestNotificationChains(t *testing.T) {
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
}

func TestNotificationKindString(t *testing.T) {
	tests := []struct {
		kind NotificationKind
		want string
	}{
		{ChainCommitted, "committed"},
		{ChainReorged, "reorged"},
		{ChainReverted, "reverted"},
		{NotificationKind(255), "unknown"},
	}

	for _, test := range tests {
		if got := test.kind.String(); got != test.want {
			t.Fatalf("%v.String() = %q, want %q", uint8(test.kind), got, test.want)
		}
	}
}

func TestNotificationApply(t *testing.T) {
	oldChain := Chain{FromBlock: 10, ToBlock: 10}
	newChain := Chain{FromBlock: 11, ToBlock: 11}
	handler := &recordingHandler{}

	notification := NewChainReorged(oldChain, newChain)
	if err := notification.Apply(context.Background(), handler); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	want := []string{"revert:10-10", "commit:11-11"}
	if !reflect.DeepEqual(handler.calls, want) {
		t.Fatalf("handler calls = %v, want %v", handler.calls, want)
	}
}

func TestNotificationLogCount(t *testing.T) {
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
	err := chain.ForEachLog(func(log types.Log) error {
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

type recordingHandler struct {
	calls []string
}

func (h *recordingHandler) Commit(ctx context.Context, chain Chain) error {
	h.calls = append(h.calls, fmt.Sprintf("commit:%d-%d", chain.FromBlock, chain.ToBlock))
	return nil
}

func (h *recordingHandler) Revert(ctx context.Context, chain Chain) error {
	h.calls = append(h.calls, fmt.Sprintf("revert:%d-%d", chain.FromBlock, chain.ToBlock))
	return nil
}
