# go-exex

`go-exex` defines small ExEx-style primitives for Ethereum indexers.

```go
type ExExHandler interface {
	HandleNotification(ctx context.Context, notification exex.ExExNotification) error
}
```

The notification model mirrors Reth's execution-extension shape:

- `ChainCommitted` carries a new canonical chain segment.
- `ChainReorged` carries the old and new chain segments.
- `ChainReverted` carries an old chain segment that was rolled back.
