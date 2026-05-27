package exex

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestRPCSourceCommitsNewHeads(t *testing.T) {
	ctx := context.Background()
	genesis := testHeader(0, common.Hash{}, "genesis")
	block1 := testHeader(1, genesis.Hash(), "a1")
	client := newFakeRPCClient(genesis, block1)
	client.logs[block1.Hash()] = []types.Log{testLog(1, block1.Hash(), 0)}

	handler := newRecordingSourceHandler()
	source := newTestRPCSource(t, client, NewMemoryStore(), StartAtBlock(0))

	if err := source.processHead(ctx, block1, handler); err != nil {
		t.Fatalf("processHead() error = %v", err)
	}

	if len(handler.calls) != 1 {
		t.Fatalf("handler calls = %d, want 1", len(handler.calls))
	}
	call := handler.calls[0]
	if call.action != "commit" {
		t.Fatalf("handler call = %s, want commit", call.action)
	}
	if call.chain.FromBlock != 1 || call.chain.ToBlock != 1 {
		t.Fatalf("committed range = %d..%d, want 1..1", call.chain.FromBlock, call.chain.ToBlock)
	}
	if len(call.chain.Blocks) != 1 || call.chain.Blocks[0].Hash != block1.Hash() {
		t.Fatalf("committed blocks = %+v, want block1 logs", call.chain.Blocks)
	}
}

func TestRPCSourceReorgsUsingStoredOldLogs(t *testing.T) {
	ctx := context.Background()
	genesis := testHeader(0, common.Hash{}, "genesis")
	block1 := testHeader(1, genesis.Hash(), "a1")
	block2 := testHeader(2, block1.Hash(), "a2")
	block2b := testHeader(2, block1.Hash(), "b2")

	client := newFakeRPCClient(genesis, block1, block2, block2b)
	client.logs[block1.Hash()] = []types.Log{testLog(1, block1.Hash(), 0)}
	client.logs[block2.Hash()] = []types.Log{testLog(2, block2.Hash(), 0)}
	client.logs[block2b.Hash()] = []types.Log{testLog(2, block2b.Hash(), 1)}

	handler := newRecordingSourceHandler()
	store := NewMemoryStore()
	source := newTestRPCSource(t, client, store, StartAtBlock(0))

	for _, header := range []*types.Header{block1, block2} {
		if err := source.processHead(ctx, header, handler); err != nil {
			t.Fatalf("processHead(%d) error = %v", header.Number.Uint64(), err)
		}
	}

	delete(client.logs, block2.Hash())
	if err := source.processHead(ctx, block2b, handler); err != nil {
		t.Fatalf("processHead(reorg) error = %v", err)
	}

	if len(handler.calls) != 4 {
		t.Fatalf("handler calls = %d, want 4", len(handler.calls))
	}
	reverted := handler.calls[2]
	committed := handler.calls[3]
	if reverted.action != "revert" || committed.action != "commit" {
		t.Fatalf("reorg calls = %s, %s; want revert, commit", reverted.action, committed.action)
	}
	if reverted.chain.FromBlock != 2 || reverted.chain.ToBlock != 2 || committed.chain.FromBlock != 2 || committed.chain.ToBlock != 2 {
		t.Fatalf("reorg ranges old=%d..%d new=%d..%d, want both 2..2", reverted.chain.FromBlock, reverted.chain.ToBlock, committed.chain.FromBlock, committed.chain.ToBlock)
	}
	if len(reverted.chain.Blocks) != 1 || reverted.chain.Blocks[0].Hash != block2.Hash() {
		t.Fatalf("old chain blocks = %+v, want old block2 logs from store", reverted.chain.Blocks)
	}
	if len(committed.chain.Blocks) != 1 || committed.chain.Blocks[0].Hash != block2b.Hash() {
		t.Fatalf("new chain blocks = %+v, want block2b logs", committed.chain.Blocks)
	}

	head, ok, err := store.Head(ctx)
	if err != nil || !ok || head.Hash != block2b.Hash() {
		t.Fatalf("store head = %+v, %v, %v; want block2b, true, nil", head, ok, err)
	}
}

func TestRPCSourceSyncSeedsStartBlockAnchor(t *testing.T) {
	ctx := context.Background()
	genesis := testHeader(0, common.Hash{}, "genesis")
	block1 := testHeader(1, genesis.Hash(), "a1")
	block2 := testHeader(2, block1.Hash(), "a2")
	client := newFakeRPCClient(genesis, block1, block2)

	handler := newRecordingSourceHandler()
	store := NewMemoryStore()
	source := newTestRPCSource(t, client, store, StartAtBlock(2))

	if err := source.Sync(ctx, handler); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if len(handler.calls) != 1 {
		t.Fatalf("handler calls = %d, want 1", len(handler.calls))
	}
	call := handler.calls[0]
	if call.action != "commit" || call.chain.FromBlock != 2 || call.chain.ToBlock != 2 {
		t.Fatalf("handler call = %+v, want commit for block 2 only", call)
	}

	anchor, ok, err := store.BlockByHash(ctx, block1.Hash())
	if err != nil || !ok || anchor.Hash != block1.Hash() {
		t.Fatalf("anchor = %+v, %v, %v; want block1 stored", anchor, ok, err)
	}
}

func TestRPCSourceSyncDefaultsToLatest(t *testing.T) {
	ctx := context.Background()
	genesis := testHeader(0, common.Hash{}, "genesis")
	block1 := testHeader(1, genesis.Hash(), "a1")
	block2 := testHeader(2, block1.Hash(), "a2")
	block3 := testHeader(3, block2.Hash(), "a3")
	client := newFakeRPCClient(genesis, block1, block2)
	client.logs[block2.Hash()] = []types.Log{testLog(2, block2.Hash(), 0)}
	client.logs[block3.Hash()] = []types.Log{testLog(3, block3.Hash(), 0)}

	handler := newRecordingSourceHandler()
	store := NewMemoryStore()
	source := newTestRPCSource(t, client, store, StartAtLatest())

	if err := source.Sync(ctx, handler); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if len(handler.calls) != 0 {
		t.Fatalf("handler calls = %d, want 0", len(handler.calls))
	}
	head, ok, err := store.Head(ctx)
	if err != nil || !ok || head.Hash != block2.Hash() {
		t.Fatalf("head = %+v, %v, %v; want block2, true, nil", head, ok, err)
	}

	client.addHeaders(block3)
	if err := source.processHead(ctx, block3, handler); err != nil {
		t.Fatalf("processHead(block3) error = %v", err)
	}
	if len(handler.calls) != 1 || handler.calls[0].action != "commit" || handler.calls[0].chain.FromBlock != 3 {
		t.Fatalf("handler calls = %+v, want one commit for block3", handler.calls)
	}
}

type handlerCall struct {
	action string
	chain  Chain
}

type recordingSourceHandler struct {
	calls []handlerCall
}

func newRecordingSourceHandler() *recordingSourceHandler {
	return &recordingSourceHandler{}
}

func (h *recordingSourceHandler) Commit(ctx context.Context, chain Chain) error {
	h.calls = append(h.calls, handlerCall{action: "commit", chain: chain})
	return nil
}

func (h *recordingSourceHandler) Revert(ctx context.Context, chain Chain) error {
	h.calls = append(h.calls, handlerCall{action: "revert", chain: chain})
	return nil
}

type fakeRPCClient struct {
	byHash    map[common.Hash]*types.Header
	canonical map[uint64]*types.Header
	logs      map[common.Hash][]types.Log
}

func newFakeRPCClient(headers ...*types.Header) *fakeRPCClient {
	client := &fakeRPCClient{
		byHash:    make(map[common.Hash]*types.Header),
		canonical: make(map[uint64]*types.Header),
		logs:      make(map[common.Hash][]types.Log),
	}
	client.addHeaders(headers...)
	return client
}

func (c *fakeRPCClient) addHeaders(headers ...*types.Header) {
	for _, header := range headers {
		c.byHash[header.Hash()] = header
		current := c.canonical[header.Number.Uint64()]
		if current == nil || string(header.Extra) > string(current.Extra) {
			c.canonical[header.Number.Uint64()] = header
		}
	}
}

func (c *fakeRPCClient) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	header := c.byHash[hash]
	if header == nil {
		return nil, fmt.Errorf("header %s not found", hash)
	}
	return header, nil
}

func (c *fakeRPCClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if number == nil {
		var latest *types.Header
		for _, header := range c.canonical {
			if latest == nil || header.Number.Uint64() > latest.Number.Uint64() {
				latest = header
			}
		}
		return latest, nil
	}
	return c.canonical[number.Uint64()], nil
}

func (c *fakeRPCClient) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.BlockHash == nil {
		return nil, fmt.Errorf("block hash is required")
	}
	return cloneLogs(c.logs[*query.BlockHash]), nil
}

func newTestRPCSource(t *testing.T, client RPCClient, store ChainStore, start RPCStart) *RPCSource {
	t.Helper()
	source, err := NewRPCSource(client, RPCSourceConfig{
		Start: start,
		Store: store,
	})
	if err != nil {
		t.Fatalf("NewRPCSource() error = %v", err)
	}
	return source
}

func testHeader(number uint64, parent common.Hash, label string) *types.Header {
	return &types.Header{
		ParentHash: parent,
		UncleHash:  types.EmptyUncleHash,
		TxHash:     types.EmptyTxsHash,
		Difficulty: big.NewInt(0),
		Number:     uint64ToBig(number),
		Time:       number,
		Extra:      []byte(label),
	}
}

func testLog(number uint64, blockHash common.Hash, index uint) types.Log {
	return types.Log{
		Address:     common.BytesToAddress([]byte("contract")),
		Topics:      []common.Hash{common.BytesToHash([]byte("topic"))},
		Data:        []byte{byte(index)},
		BlockNumber: number,
		BlockHash:   blockHash,
		TxHash:      common.BytesToHash([]byte{byte(number), byte(index)}),
		Index:       index,
	}
}

func TestCloneLogsDeepCopiesSlices(t *testing.T) {
	logs := []types.Log{testLog(1, common.BytesToHash([]byte("block")), 0)}
	cloned := cloneLogs(logs)
	logs[0].Topics[0] = common.BytesToHash([]byte("changed"))
	logs[0].Data[0] = 9

	if reflect.DeepEqual(cloned, logs) {
		t.Fatalf("cloneLogs() shared mutable slices")
	}
}
