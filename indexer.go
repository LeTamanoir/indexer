package indexer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	defaultBatchSize  = uint64(2_000)
	defaultReorgDepth = uint64(128)
)

var (
	// ErrReorgRequiresReset is returned when a reorg invalidates the current
	// indexer state, no checkpoint can be loaded, and the handler cannot reset.
	ErrReorgRequiresReset = errors.New("indexer: automatic reorg recovery requires checkpointing or a resettable handler")
)

// Handler owns the caller's indexed state.
//
// Filter returns the base Ethereum log filter. Sync methods set FromBlock and
// ToBlock for each requested batch while preserving addresses, topics, and
// other filter fields.
//
// HandleLogs receives logs sorted by transaction/log index and grouped by
// block. A single call never contains logs from multiple blocks.
type Handler interface {
	Filter() ethereum.FilterQuery
	HandleLogs(ctx context.Context, logs []types.Log) error
}

// Resetter can be implemented by handlers that know how to clear their state.
// It is used for automatic reorg recovery when checkpointing cannot restore a
// clean state.
type Resetter interface {
	Reset()
}

// Indexer coordinates log fetching, optional cache/checkpoint storage, and
// delivery to a Handler.
type Indexer[T Handler] struct {
	client  *ethclient.Client
	handler T
	opts    options

	head           *types.Header
	lastCheckpoint uint64
	blockHashes    map[uint64]common.Hash
}

// New creates an Indexer.
func New[T Handler](client *ethclient.Client, handler T, opts ...Option) (*Indexer[T], error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}

	if client == nil {
		return nil, errors.New("indexer: client must not be nil")
	}
	if isNilHandler(handler) {
		return nil, errors.New("indexer: handler must not be nil")
	}
	if cfg.batchSize == 0 {
		return nil, errors.New("indexer: batch size must be greater than zero")
	}
	if cfg.reorgDepth == 0 {
		return nil, errors.New("indexer: reorg depth must be greater than zero")
	}
	if cfg.checkpointStore != nil && cfg.checkpointInterval == 0 {
		return nil, errors.New("indexer: checkpoint interval must be greater than zero when checkpointing is enabled")
	}
	if cfg.retry.InitialBackoff <= 0 {
		return nil, errors.New("indexer: retry initial backoff must be greater than zero")
	}
	if cfg.retry.MaxBackoff <= 0 {
		return nil, errors.New("indexer: retry max backoff must be greater than zero")
	}
	if cfg.retry.MaxBackoff < cfg.retry.InitialBackoff {
		return nil, errors.New("indexer: retry max backoff must be greater than or equal to initial backoff")
	}
	if cfg.retry.Retryable == nil {
		return nil, errors.New("indexer: retry policy must include a retryable function")
	}
	if cfg.logCache == nil {
		cfg.logCache = NoLogCache()
	}
	if cfg.logger == nil {
		cfg.logger = silentLogger()
	}

	return &Indexer[T]{
		client:      client,
		handler:     handler,
		opts:        cfg,
		blockHashes: make(map[uint64]common.Hash, cfg.reorgDepth),
	}, nil
}

// Handler returns the handler currently owned by the indexer. If a checkpoint
// is loaded, this may be a restored handler value.
func (i *Indexer[T]) Handler() T {
	return i.handler
}

// Head returns the latest header successfully indexed by this Indexer. The
// returned pointer must be treated as read-only.
func (i *Indexer[T]) Head() *types.Header {
	return i.head
}

// Sync is a compatibility alias for SyncTo.
func (i *Indexer[T]) Sync(ctx context.Context, head *types.Header) error {
	return i.SyncTo(ctx, head)
}

// SyncTo indexes logs up to and including head.
func (i *Indexer[T]) SyncTo(ctx context.Context, head *types.Header) error {
	if err := validateHeader(head); err != nil {
		return err
	}

	target := head.Number.Uint64()
	if i.checkForReorg(head) {
		if err := i.recoverFromReorg(ctx, head); err != nil {
			return err
		}
	}

	if i.head == nil {
		if _, err := i.loadCheckpoint(ctx, target); err != nil {
			return err
		}
	}

	from := i.fromBlock()
	if target < from {
		return nil
	}

	if err := i.fetchAndProcessLogs(ctx, from, target, target); err != nil {
		i.invalidateHead()
		return err
	}

	i.recordHead(head)
	i.cleanupBlockHashes(head)
	return nil
}

// SyncRange indexes logs from from to to, inclusive, and records the header for
// to after the range succeeds.
func (i *Indexer[T]) SyncRange(ctx context.Context, from, to uint64) error {
	if to < from {
		return nil
	}

	head, err := i.client.HeaderByNumber(ctx, new(big.Int).SetUint64(to))
	if err != nil {
		return fmt.Errorf("indexer: fetch range head %d: %w", to, err)
	}

	if err := i.fetchAndProcessLogs(ctx, from, to, to); err != nil {
		i.invalidateHead()
		return err
	}

	i.recordHead(head)
	i.cleanupBlockHashes(head)
	return nil
}

// SyncWithRetry calls SyncTo and retries failures according to the configured
// RetryPolicy. A non-positive MaxAttempts means retry until the context ends.
func (i *Indexer[T]) SyncWithRetry(ctx context.Context, head *types.Header) error {
	_, err := i.withRetry(ctx, func() error {
		return i.SyncTo(ctx, head)
	})
	return err
}

func (i *Indexer[T]) recoverFromReorg(ctx context.Context, head *types.Header) error {
	target := head.Number.Uint64()
	reorgFrom := target
	if target > 0 {
		reorgFrom = target - 1
	}

	expectedHash := i.blockHashes[reorgFrom]
	stats := ReorgStats{
		Block:        target,
		ParentBlock:  reorgFrom,
		ParentHash:   head.ParentHash,
		ExpectedHash: expectedHash,
	}

	i.invalidateHead()
	if i.opts.checkpointStore != nil {
		if err := i.opts.checkpointStore.Prune(ctx, reorgFrom); err != nil {
			return fmt.Errorf("indexer: prune checkpoints after reorg: %w", err)
		}
	}

	if loaded, err := i.loadCheckpoint(ctx, reorgFrom); err != nil {
		return err
	} else if loaded {
		stats.CheckpointLoaded = true
		if i.head != nil {
			stats.CheckpointBlock = i.head.Number.Uint64()
		}
		i.emitReorg(stats)
		return nil
	}

	resetter, ok := any(i.handler).(Resetter)
	if !ok {
		i.emitReorg(stats)
		return ErrReorgRequiresReset
	}
	resetter.Reset()
	stats.Reset = true
	i.emitReorg(stats)
	return nil
}

func (i *Indexer[T]) fetchAndProcessLogs(ctx context.Context, fromBlock, toBlock, headBlock uint64) error {
	for _, b := range makeBatches(i.opts.batchSize, fromBlock, toBlock) {
		start := time.Now()
		logs, cacheHit, retryCount, err := i.fetchLogBatch(ctx, b.from, b.to, headBlock)
		if err != nil {
			return err
		}

		if len(logs) > 0 {
			if err := i.processLogBatch(ctx, logs); err != nil {
				return err
			}
			if err := i.checkpoint(ctx, logs[len(logs)-1].BlockNumber); err != nil {
				return err
			}
		}

		i.emitBatch(BatchStats{
			FromBlock:  b.from,
			ToBlock:    b.to,
			LogCount:   len(logs),
			Duration:   time.Since(start),
			CacheHit:   cacheHit,
			RetryCount: retryCount,
		})
	}
	return nil
}

func (i *Indexer[T]) fetchLogBatch(ctx context.Context, fromBlock, toBlock, headBlock uint64) ([]types.Log, bool, int, error) {
	logs, ok, err := i.opts.logCache.Load(ctx, fromBlock, toBlock)
	if err != nil {
		return nil, false, 0, fmt.Errorf("indexer: load log cache %d-%d: %w", fromBlock, toBlock, err)
	}
	if ok {
		return logs, true, 0, nil
	}

	query := i.handler.Filter()
	query.FromBlock = new(big.Int).SetUint64(fromBlock)
	query.ToBlock = new(big.Int).SetUint64(toBlock)

	var fetched []types.Log
	retryCount, err := i.withRetry(ctx, func() error {
		var err error
		fetched, err = i.client.FilterLogs(ctx, query)
		return err
	})
	if err != nil {
		return nil, false, retryCount, fmt.Errorf("indexer: filter logs from %d to %d: %w", fromBlock, toBlock, err)
	}

	if headBlock > toBlock && headBlock-toBlock > i.opts.reorgDepth {
		if err := i.opts.logCache.Save(ctx, fromBlock, toBlock, fetched); err != nil {
			return nil, false, retryCount, fmt.Errorf("indexer: save log cache %d-%d: %w", fromBlock, toBlock, err)
		}
	}

	return fetched, false, retryCount, nil
}

func (i *Indexer[T]) processLogBatch(ctx context.Context, logs []types.Log) error {
	slices.SortFunc(logs, func(a, b types.Log) int {
		if a.BlockNumber != b.BlockNumber {
			return cmp.Compare(a.BlockNumber, b.BlockNumber)
		}
		return cmp.Compare(a.Index, b.Index)
	})

	for len(logs) > 0 {
		blockNumber := logs[0].BlockNumber
		end := 1
		for end < len(logs) && logs[end].BlockNumber == blockNumber {
			end++
		}

		if err := i.handler.HandleLogs(ctx, logs[:end]); err != nil {
			return err
		}
		logs = logs[end:]
	}

	return nil
}

func (i *Indexer[T]) checkpoint(ctx context.Context, blockNumber uint64) error {
	if i.opts.checkpointStore == nil {
		return nil
	}
	if i.lastCheckpoint != 0 && blockNumber-i.lastCheckpoint < i.opts.checkpointInterval {
		return nil
	}

	start := time.Now()
	if err := i.opts.checkpointStore.Save(ctx, blockNumber, i.handler); err != nil {
		return fmt.Errorf("indexer: save checkpoint at %d: %w", blockNumber, err)
	}
	i.lastCheckpoint = blockNumber
	i.emitCheckpoint(CheckpointStats{
		Block:    blockNumber,
		Duration: time.Since(start),
		Store:    describeStore(i.opts.checkpointStore),
	})
	return nil
}

func (i *Indexer[T]) loadCheckpoint(ctx context.Context, target uint64) (bool, error) {
	if i.opts.checkpointStore == nil {
		return false, nil
	}

	start := time.Now()
	block, state, ok, err := i.opts.checkpointStore.Load(ctx, target, i.handler)
	if err != nil {
		return false, fmt.Errorf("indexer: load checkpoint at or before %d: %w", target, err)
	}
	if !ok {
		return false, nil
	}

	handler, ok := state.(T)
	if !ok {
		return false, fmt.Errorf("indexer: checkpoint store returned %T, want handler type %T", state, i.handler)
	}

	head, err := i.client.HeaderByNumber(ctx, new(big.Int).SetUint64(block))
	if err != nil {
		return false, fmt.Errorf("indexer: fetch checkpoint header %d: %w", block, err)
	}

	i.handler = handler
	i.head = head
	i.blockHashes[block] = head.Hash()
	i.lastCheckpoint = block
	i.emitCheckpoint(CheckpointStats{
		Block:    block,
		Duration: time.Since(start),
		Store:    describeStore(i.opts.checkpointStore),
		Loaded:   true,
	})
	return true, nil
}

func (i *Indexer[T]) checkForReorg(head *types.Header) bool {
	if head.Number.Sign() == 0 {
		return false
	}

	parentBlockNum := head.Number.Uint64() - 1
	expectedHash, exists := i.blockHashes[parentBlockNum]
	return exists && head.ParentHash != expectedHash
}

func (i *Indexer[T]) invalidateHead() {
	i.head = nil
	i.lastCheckpoint = 0
	i.blockHashes = make(map[uint64]common.Hash, i.opts.reorgDepth)
}

func (i *Indexer[T]) cleanupBlockHashes(head *types.Header) {
	blockNumber := head.Number.Uint64()
	if blockNumber <= i.opts.reorgDepth {
		return
	}

	minBlock := blockNumber - i.opts.reorgDepth
	for block := range i.blockHashes {
		if block < minBlock {
			delete(i.blockHashes, block)
		}
	}
}

func (i *Indexer[T]) recordHead(head *types.Header) {
	i.head = head
	i.blockHashes[head.Number.Uint64()] = head.Hash()
}

func (i *Indexer[T]) fromBlock() uint64 {
	if i.head == nil {
		return i.opts.startBlock
	}
	return i.head.Number.Uint64() + 1
}

func (i *Indexer[T]) withRetry(ctx context.Context, fn func() error) (int, error) {
	backoff := i.opts.retry.InitialBackoff
	attempt := 0
	retries := 0

	for {
		attempt++
		err := fn()
		if err == nil {
			return retries, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return retries, ctxErr
		}
		if i.opts.retry.MaxAttempts > 0 && attempt >= i.opts.retry.MaxAttempts {
			return retries, err
		}
		if !i.opts.retry.Retryable(err) {
			return retries, err
		}

		i.opts.logger.Warn("retrying indexer operation",
			slog.Duration("backoff", backoff),
			slog.Int("attempt", attempt),
			slog.Any("error", err))

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return retries, ctx.Err()
		}

		retries++
		backoff *= 2
		if backoff > i.opts.retry.MaxBackoff {
			backoff = i.opts.retry.MaxBackoff
		}
	}
}

func (i *Indexer[T]) emitBatch(stats BatchStats) {
	if i.opts.hooks.OnBatch != nil {
		i.opts.hooks.OnBatch(stats)
	}
}

func (i *Indexer[T]) emitCheckpoint(stats CheckpointStats) {
	if i.opts.hooks.OnCheckpoint != nil {
		i.opts.hooks.OnCheckpoint(stats)
	}
}

func (i *Indexer[T]) emitReorg(stats ReorgStats) {
	i.opts.logger.Warn("chain reorg detected",
		slog.Uint64("block", stats.Block),
		slog.Uint64("parent_block", stats.ParentBlock),
		slog.String("parent_hash", stats.ParentHash.Hex()),
		slog.String("expected_hash", stats.ExpectedHash.Hex()))

	if i.opts.hooks.OnReorg != nil {
		i.opts.hooks.OnReorg(stats)
	}
}

func validateHeader(head *types.Header) error {
	if head == nil {
		return errors.New("indexer: head must not be nil")
	}
	if head.Number == nil {
		return errors.New("indexer: head number must not be nil")
	}
	return nil
}

func isNilHandler[T Handler](handler T) bool {
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

type batch struct {
	from uint64
	to   uint64
}

func makeBatches(size, from, to uint64) []batch {
	batches := make([]batch, 0, ((to-from)/size)+1)
	for from <= to {
		batchTo := from + size - 1
		if batchTo < from || batchTo > to {
			batchTo = to
		}
		batches = append(batches, batch{from: from, to: batchTo})
		from = batchTo + 1
	}
	return batches
}
