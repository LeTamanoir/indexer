# indexer

`indexer` is a small Go package for indexing Ethereum logs.

```go
idx, err := indexer.New(client, handler,
	indexer.WithStartBlock(18_000_000),
	indexer.WithBatchSize(2_000),
	indexer.WithCheckpointInterval(10_000),
	indexer.WithCheckpointStore(indexer.FileCheckpoints(".cache/aave")),
	indexer.WithLogCache(indexer.FileLogCache(".cache/aave")),
	indexer.WithLogger(logger),
)
if err != nil {
	return err
}

if err := idx.SyncTo(ctx, head); err != nil {
	return err
}
```

Handlers provide a full `ethereum.FilterQuery` and receive sorted logs grouped
by block:

```go
type Handler interface {
	Filter() ethereum.FilterQuery
	HandleLogs(ctx context.Context, logs []types.Log) error
}
```

Checkpoint and log-cache storage are optional. The default writes nothing to
disk. Automatic reorg recovery requires checkpointing or a handler that
implements:

```go
type Resetter interface {
	Reset()
}
```
