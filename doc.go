// Package indexer provides tools for indexing Ethereum logs while letting
// applications own their indexed state and storage policy.
//
// The package does not subscribe to new heads. Callers provide headers with
// SyncTo or explicit ranges with SyncRange.
//
// Logs are fetched in block ranges, sorted by BlockNumber and Index, and passed
// to the handler grouped by block. A HandleLogs call never contains logs from
// more than one block.
//
// Checkpointing and log caching are disabled by default. FileCheckpoints and
// FileLogCache are provided for simple local persistence, but callers may
// implement CheckpointStore and LogCache with databases or other storage.
//
// Automatic reorg recovery requires either checkpointing or a handler that
// implements Resetter. If a reorg is detected and no checkpoint can be loaded,
// a resettable handler is reset and replay starts from StartBlock. Without one,
// SyncTo returns an error so the caller can choose how to recover.
//
// Log cache entries are saved only for batches whose end block is older than
// ReorgDepth relative to the target head. This avoids caching logs from blocks
// that are still likely to be reorganized.
package indexer
