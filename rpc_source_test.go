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

	handler := newRecordingHandler()
	source := newTestRPCSource(t, client, NewMemoryStore(), 0)

	if err := source.ProcessHead(ctx, block1, handler); err != nil {
		t.Fatalf("ProcessHead() error = %v", err)
	}

	if len(handler.notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(handler.notifications))
	}
	notification := handler.notifications[0]
	if notification.Kind != ChainCommitted {
		t.Fatalf("notification kind = %v, want ChainCommitted", notification.Kind)
	}
	if notification.New.FromBlock != 1 || notification.New.ToBlock != 1 {
		t.Fatalf("committed range = %d..%d, want 1..1", notification.New.FromBlock, notification.New.ToBlock)
	}
	if len(notification.New.Blocks) != 1 || notification.New.Blocks[0].Hash != block1.Hash() {
		t.Fatalf("committed blocks = %+v, want block1 logs", notification.New.Blocks)
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

	handler := newRecordingHandler()
	store := NewMemoryStore()
	source := newTestRPCSource(t, client, store, 0)

	for _, header := range []*types.Header{block1, block2} {
		if err := source.ProcessHead(ctx, header, handler); err != nil {
			t.Fatalf("ProcessHead(%d) error = %v", header.Number.Uint64(), err)
		}
	}

	delete(client.logs, block2.Hash())
	if err := source.ProcessHead(ctx, block2b, handler); err != nil {
		t.Fatalf("ProcessHead(reorg) error = %v", err)
	}

	if len(handler.notifications) != 3 {
		t.Fatalf("notifications = %d, want 3", len(handler.notifications))
	}
	reorg := handler.notifications[2]
	if reorg.Kind != ChainReorged {
		t.Fatalf("reorg kind = %v, want ChainReorged", reorg.Kind)
	}
	if reorg.Old.FromBlock != 2 || reorg.Old.ToBlock != 2 || reorg.New.FromBlock != 2 || reorg.New.ToBlock != 2 {
		t.Fatalf("reorg ranges old=%d..%d new=%d..%d, want both 2..2", reorg.Old.FromBlock, reorg.Old.ToBlock, reorg.New.FromBlock, reorg.New.ToBlock)
	}
	if len(reorg.Old.Blocks) != 1 || reorg.Old.Blocks[0].Hash != block2.Hash() {
		t.Fatalf("old chain blocks = %+v, want old block2 logs from store", reorg.Old.Blocks)
	}
	if len(reorg.New.Blocks) != 1 || reorg.New.Blocks[0].Hash != block2b.Hash() {
		t.Fatalf("new chain blocks = %+v, want block2b logs", reorg.New.Blocks)
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

	handler := newRecordingHandler()
	store := NewMemoryStore()
	source := newTestRPCSource(t, client, store, 2)

	if err := source.Sync(ctx, handler); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if len(handler.notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(handler.notifications))
	}
	notification := handler.notifications[0]
	if notification.Kind != ChainCommitted || notification.New.FromBlock != 2 || notification.New.ToBlock != 2 {
		t.Fatalf("notification = %+v, want commit for block 2 only", notification)
	}

	anchor, ok, err := store.CanonicalBlock(ctx, 1)
	if err != nil || !ok || anchor.Hash != block1.Hash() {
		t.Fatalf("anchor = %+v, %v, %v; want block1 stored canonically", anchor, ok, err)
	}
}

type recordingHandler struct {
	notifications []ExExNotification
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{}
}

func (h *recordingHandler) HandleNotification(ctx context.Context, notification ExExNotification) error {
	h.notifications = append(h.notifications, notification)
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
	for _, header := range headers {
		client.byHash[header.Hash()] = header
		current := client.canonical[header.Number.Uint64()]
		if current == nil || string(header.Extra) > string(current.Extra) {
			client.canonical[header.Number.Uint64()] = header
		}
	}
	return client
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
		return nil, fmt.Errorf("BlockHash is required")
	}
	return cloneLogs(c.logs[*query.BlockHash]), nil
}

func newTestRPCSource(t *testing.T, client RPCClient, store ChainStore, startBlock uint64) *RPCSource {
	t.Helper()
	source, err := NewRPCSource(client, RPCSourceConfig{
		StartBlock: startBlock,
		Store:      store,
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
