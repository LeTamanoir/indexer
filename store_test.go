package exex

import (
	"context"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestMemoryStore(t *testing.T) {
	testChainStore(t, NewMemoryStore())
}

func testChainStore(t *testing.T, store ChainStore) {
	t.Helper()
	ctx := context.Background()

	genesis := testStoredBlock(0, common.Hash{}, "genesis", nil)
	block1 := testStoredBlock(1, genesis.Hash, "a1", []types.Log{testLog(1, genesis.Hash, 0)})
	block2 := testStoredBlock(2, block1.Hash, "a2", []types.Log{testLog(2, block1.Hash, 0)})
	block2b := testStoredBlock(2, block1.Hash, "b2", []types.Log{testLog(2, block1.Hash, 1)})

	if err := store.UpdateCanonicalChain(ctx, nil, []StoredBlock{genesis, block1, block2}); err != nil {
		t.Fatalf("UpdateCanonicalChain() error = %v", err)
	}

	head, ok, err := store.Head(ctx)
	if err != nil || !ok || head.Hash != block2.Hash {
		t.Fatalf("Head() = %+v, %v, %v; want block2, true, nil", head, ok, err)
	}

	byHash, ok, err := store.BlockByHash(ctx, block2.Hash)
	if err != nil || !ok || !reflect.DeepEqual(byHash.Logs, block2.Logs) {
		t.Fatalf("BlockByHash(block2) = %+v, %v, %v; want block2 logs, true, nil", byHash, ok, err)
	}

	if err := store.UpdateCanonicalChain(ctx, []StoredBlock{block2}, []StoredBlock{block2b}); err != nil {
		t.Fatalf("reorg UpdateCanonicalChain() error = %v", err)
	}

	head, ok, err = store.Head(ctx)
	if err != nil || !ok || head.Hash != block2b.Hash {
		t.Fatalf("Head() after reorg = %+v, %v, %v; want block2b, true, nil", head, ok, err)
	}

	oldBlock, ok, err := store.BlockByHash(ctx, block2.Hash)
	if err != nil || !ok || oldBlock.Hash != block2.Hash {
		t.Fatalf("BlockByHash(old block2) after reorg = %+v, %v, %v; want old block retained", oldBlock, ok, err)
	}
}

func testStoredBlock(number uint64, parent common.Hash, label string, logs []types.Log) StoredBlock {
	hash := common.BytesToHash([]byte(label))
	for i := range logs {
		logs[i].BlockNumber = number
		logs[i].BlockHash = hash
	}
	return StoredBlock{
		Number:     number,
		Hash:       hash,
		ParentHash: parent,
		Logs:       logs,
	}
}
