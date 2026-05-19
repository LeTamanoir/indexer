package exex

import (
	"reflect"
	"testing"
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
