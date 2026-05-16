package indexer

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type testHandler struct {
	Count      int
	filter     ethereum.FilterQuery
	handled    [][]types.Log
	handleErr  error
	resetCount int
}

func (h *testHandler) Filter() ethereum.FilterQuery {
	return h.filter
}

func (h *testHandler) HandleLogs(_ context.Context, logs []types.Log) error {
	if h.handleErr != nil {
		return h.handleErr
	}
	h.Count += len(logs)
	cp := make([]types.Log, len(logs))
	copy(cp, logs)
	h.handled = append(h.handled, cp)
	return nil
}

func (h *testHandler) Reset() {
	h.Count = 0
	h.handled = nil
	h.resetCount++
}

type nonResetHandler struct {
	filter ethereum.FilterQuery
}

func (h *nonResetHandler) Filter() ethereum.FilterQuery {
	return h.filter
}

func (h *nonResetHandler) HandleLogs(context.Context, []types.Log) error {
	return nil
}

func makeHeader(number uint64) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(number)}
}

func makeHeaderWithParent(number uint64, parent common.Hash) *types.Header {
	return &types.Header{
		Number:     new(big.Int).SetUint64(number),
		ParentHash: parent,
	}
}

func newTestIndexer(t *testing.T, handler *testHandler, opts ...Option) *Indexer[*testHandler] {
	t.Helper()
	idx, err := New(&ethclient.Client{}, handler, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestNewValidation(t *testing.T) {
	handler := &testHandler{}

	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{
			name: "nil client",
			run: func() error {
				_, err := New(nil, handler)
				return err
			},
			want: "client must not be nil",
		},
		{
			name: "typed nil handler",
			run: func() error {
				var nilHandler *testHandler
				_, err := New(&ethclient.Client{}, nilHandler)
				return err
			},
			want: "handler must not be nil",
		},
		{
			name: "zero batch size",
			run: func() error {
				_, err := New(&ethclient.Client{}, handler, WithBatchSize(0))
				return err
			},
			want: "batch size",
		},
		{
			name: "zero reorg depth",
			run: func() error {
				_, err := New(&ethclient.Client{}, handler, WithReorgDepth(0))
				return err
			},
			want: "reorg depth",
		},
		{
			name: "checkpoint interval required",
			run: func() error {
				_, err := New(&ethclient.Client{}, handler, WithCheckpointStore(FileCheckpoints(t.TempDir())))
				return err
			},
			want: "checkpoint interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestNoCheckpointsOptionDisablesCheckpointing(t *testing.T) {
	_, err := New(&ethclient.Client{}, &testHandler{}, WithCheckpointStore(NoCheckpoints()))
	if err != nil {
		t.Fatal(err)
	}
}

func TestMakeBatches(t *testing.T) {
	got := makeBatches(10, 100, 125)
	want := []batch{{from: 100, to: 109}, {from: 110, to: 119}, {from: 120, to: 125}}

	if len(got) != len(want) {
		t.Fatalf("got %d batches, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("batch %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestProcessLogBatchSortsAndGroupsByBlock(t *testing.T) {
	handler := &testHandler{}
	idx := newTestIndexer(t, handler)

	err := idx.processLogBatch(context.Background(), []types.Log{
		{BlockNumber: 3, Index: 1},
		{BlockNumber: 1, Index: 2},
		{BlockNumber: 1, Index: 0},
		{BlockNumber: 3, Index: 0},
		{BlockNumber: 2, Index: 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(handler.handled) != 3 {
		t.Fatalf("got %d HandleLogs calls, want 3", len(handler.handled))
	}
	if handler.handled[0][0].BlockNumber != 1 || handler.handled[0][0].Index != 0 || handler.handled[0][1].Index != 2 {
		t.Fatalf("first block not sorted/grouped correctly: %+v", handler.handled[0])
	}
	if handler.handled[1][0].BlockNumber != 2 {
		t.Fatalf("second call should be block 2: %+v", handler.handled[1])
	}
	if handler.handled[2][0].BlockNumber != 3 || handler.handled[2][0].Index != 0 || handler.handled[2][1].Index != 1 {
		t.Fatalf("third block not sorted/grouped correctly: %+v", handler.handled[2])
	}
}

func TestProcessLogBatchReturnsHandlerError(t *testing.T) {
	handler := &testHandler{handleErr: errors.New("boom")}
	idx := newTestIndexer(t, handler)

	err := idx.processLogBatch(context.Background(), []types.Log{{BlockNumber: 1}})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("got %v, want boom", err)
	}
}

func TestFileCheckpointsLoadClosestAndPrune(t *testing.T) {
	ctx := context.Background()
	store := FileCheckpoints(t.TempDir())

	for _, tc := range []struct {
		block uint64
		count int
	}{
		{block: 100, count: 1},
		{block: 200, count: 2},
		{block: 300, count: 3},
	} {
		if err := store.Save(ctx, tc.block, &testHandler{Count: tc.count}); err != nil {
			t.Fatal(err)
		}
	}

	block, state, ok, err := store.Load(ctx, 250, &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	loaded := state.(*testHandler)
	if !ok || block != 200 || loaded.Count != 2 {
		t.Fatalf("got block=%d ok=%v count=%d, want block=200 ok=true count=2", block, ok, loaded.Count)
	}

	if err := store.Prune(ctx, 200); err != nil {
		t.Fatal(err)
	}

	block, state, ok, err = store.Load(ctx, 350, &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	loaded = state.(*testHandler)
	if !ok || block != 100 || loaded.Count != 1 {
		t.Fatalf("after prune got block=%d ok=%v count=%d, want block=100 ok=true count=1", block, ok, loaded.Count)
	}
}

func TestFileLogCacheRoundTrip(t *testing.T) {
	ctx := context.Background()
	cache := FileLogCache(t.TempDir())
	logs := []types.Log{{BlockNumber: 100, Index: 2}}

	got, ok, err := cache.Load(ctx, 100, 109)
	if err != nil {
		t.Fatal(err)
	}
	if ok || got != nil {
		t.Fatalf("empty cache got ok=%v logs=%v", ok, got)
	}

	if err := cache.Save(ctx, 100, 109, logs); err != nil {
		t.Fatal(err)
	}

	got, ok, err = cache.Load(ctx, 100, 109)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(got) != 1 || got[0].BlockNumber != 100 || got[0].Index != 2 {
		t.Fatalf("unexpected cache load: ok=%v logs=%+v", ok, got)
	}
}

func TestCheckpointIntervalAndHook(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var checkpoints []CheckpointStats
	handler := &testHandler{Count: 7}
	idx := newTestIndexer(t, handler,
		WithCheckpointStore(FileCheckpoints(dir)),
		WithCheckpointInterval(100),
		WithHooks(Hooks{
			OnCheckpoint: func(stats CheckpointStats) {
				checkpoints = append(checkpoints, stats)
			},
		}),
	)

	if err := idx.checkpoint(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if err := idx.checkpoint(ctx, 50); err != nil {
		t.Fatal(err)
	}
	if err := idx.checkpoint(ctx, 110); err != nil {
		t.Fatal(err)
	}

	if len(checkpoints) != 2 {
		t.Fatalf("got %d checkpoint hooks, want 2", len(checkpoints))
	}

	block, state, ok, err := FileCheckpoints(dir).Load(ctx, 110, &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	loaded := state.(*testHandler)
	if !ok || block != 110 || loaded.Count != 7 {
		t.Fatalf("got block=%d ok=%v count=%d, want block=110 ok=true count=7", block, ok, loaded.Count)
	}
}

func TestFetchAndProcessLogsFromCacheEmitsBatchStats(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	cache := FileLogCache(cacheDir)
	if err := cache.Save(ctx, 100, 109, []types.Log{
		{BlockNumber: 101, Index: 1},
		{BlockNumber: 101, Index: 0},
	}); err != nil {
		t.Fatal(err)
	}

	var batches []BatchStats
	handler := &testHandler{}
	idx := newTestIndexer(t, handler,
		WithBatchSize(10),
		WithLogCache(cache),
		WithHooks(Hooks{
			OnBatch: func(stats BatchStats) {
				batches = append(batches, stats)
			},
		}),
	)

	if err := idx.fetchAndProcessLogs(ctx, 100, 109, 1_000); err != nil {
		t.Fatal(err)
	}

	if len(handler.handled) != 1 || len(handler.handled[0]) != 2 {
		t.Fatalf("unexpected handled logs: %+v", handler.handled)
	}
	if len(batches) != 1 || !batches[0].CacheHit || batches[0].LogCount != 2 {
		t.Fatalf("unexpected batch stats: %+v", batches)
	}
}

func TestReorgWithoutCheckpointRequiresResetter(t *testing.T) {
	handler := &nonResetHandler{}
	idx, err := New(&ethclient.Client{}, handler, WithStartBlock(100))
	if err != nil {
		t.Fatal(err)
	}

	oldHead := makeHeader(10)
	idx.recordHead(oldHead)

	err = idx.SyncTo(context.Background(), makeHeaderWithParent(11, common.HexToHash("0xbad")))
	if !errors.Is(err, ErrReorgRequiresReset) {
		t.Fatalf("got %v, want ErrReorgRequiresReset", err)
	}
}

func TestReorgWithResetterResetsAndReplaysFromStart(t *testing.T) {
	handler := &testHandler{Count: 42}
	idx := newTestIndexer(t, handler, WithStartBlock(100))
	oldHead := makeHeader(10)
	idx.recordHead(oldHead)

	err := idx.SyncTo(context.Background(), makeHeaderWithParent(11, common.HexToHash("0xbad")))
	if err != nil {
		t.Fatal(err)
	}
	if handler.resetCount != 1 || handler.Count != 0 {
		t.Fatalf("resetCount=%d count=%d, want resetCount=1 count=0", handler.resetCount, handler.Count)
	}
	if idx.Head() != nil {
		t.Fatal("head should stay nil when target is before start block after reorg")
	}
}
