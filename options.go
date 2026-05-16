package indexer

import (
	"io"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Option configures an Indexer.
type Option func(*options)

type options struct {
	startBlock         uint64
	batchSize          uint64
	checkpointInterval uint64
	reorgDepth         uint64
	checkpointStore    CheckpointStore
	logCache           LogCache
	logger             *slog.Logger
	retry              RetryPolicy
	hooks              Hooks
}

func defaultOptions() options {
	return options{
		batchSize:  defaultBatchSize,
		reorgDepth: defaultReorgDepth,
		logCache:   NoLogCache(),
		logger:     silentLogger(),
		retry:      DefaultRetryPolicy(),
	}
}

// WithStartBlock sets the block used when there is no current head and no
// checkpoint can be loaded.
func WithStartBlock(block uint64) Option {
	return func(o *options) {
		o.startBlock = block
	}
}

// WithBatchSize sets the inclusive block span used for each FilterLogs call.
func WithBatchSize(size uint64) Option {
	return func(o *options) {
		o.batchSize = size
	}
}

// WithCheckpointInterval controls how often checkpoints are saved. It must be
// greater than zero when checkpointing is enabled.
func WithCheckpointInterval(blocks uint64) Option {
	return func(o *options) {
		o.checkpointInterval = blocks
	}
}

// WithReorgDepth controls block hash retention and determines when log cache
// writes are safe.
func WithReorgDepth(blocks uint64) Option {
	return func(o *options) {
		o.reorgDepth = blocks
	}
}

// WithCheckpointStore enables checkpointing with store. Pass nil to disable it.
func WithCheckpointStore(store CheckpointStore) Option {
	return func(o *options) {
		if _, ok := store.(disabledCheckpointStore); ok {
			store = nil
		}
		o.checkpointStore = store
	}
}

// WithLogCache configures an optional log cache. Passing nil disables caching.
func WithLogCache(cache LogCache) Option {
	return func(o *options) {
		if cache == nil {
			cache = NoLogCache()
		}
		o.logCache = cache
	}
}

// WithLogger configures indexer logging. Passing nil keeps logging silent.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger == nil {
			logger = silentLogger()
		}
		o.logger = logger
	}
}

// WithRetryPolicy configures retry behavior for SyncWithRetry and FilterLogs.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(o *options) {
		o.retry = policy
	}
}

// WithHooks configures optional progress callbacks.
func WithHooks(hooks Hooks) Option {
	return func(o *options) {
		o.hooks = hooks
	}
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Hooks contains optional observability callbacks.
type Hooks struct {
	OnBatch      func(BatchStats)
	OnCheckpoint func(CheckpointStats)
	OnReorg      func(ReorgStats)
}

// BatchStats describes one fetched and processed batch.
type BatchStats struct {
	FromBlock  uint64
	ToBlock    uint64
	LogCount   int
	Duration   time.Duration
	CacheHit   bool
	RetryCount int
}

// CheckpointStats describes a checkpoint save or load.
type CheckpointStats struct {
	Block    uint64
	Duration time.Duration
	Store    string
	Loaded   bool
}

// ReorgStats describes a detected reorg and the recovery path taken.
type ReorgStats struct {
	Block            uint64
	ParentBlock      uint64
	ParentHash       common.Hash
	ExpectedHash     common.Hash
	CheckpointLoaded bool
	CheckpointBlock  uint64
	Reset            bool
}
