# go-exex

`go-exex` defines ExEx-style primitives for Ethereum indexers.

```go
type ExExHandler interface {
	HandleNotification(ctx context.Context, notification exex.ExExNotification) error
}
```

Handlers can implement the lower-level `ExExHandler` interface directly, or use
`ChainHandler` when they only need apply/rollback hooks:

```go
type ChainHandler interface {
	CommitChain(ctx context.Context, chain exex.Chain) error
	RevertChain(ctx context.Context, chain exex.Chain) error
}
```

The notification model mirrors Reth's execution-extension shape:

- `ChainCommitted` carries a new canonical chain segment.
- `ChainReorged` carries the old and new chain segments.
- `ChainReverted` carries an old chain segment that was rolled back.

`RPCSource` can drive a handler from a go-ethereum RPC client. It tracks headers,
fetches logs by block hash, stores observed chain state, and synthesizes reorg
notifications.

```go
store, err := exex.NewSQLiteStore("exex.sqlite")
source, err := exex.NewRPCSource(client, exex.RPCSourceConfig{
	StartBlock: 12_345_000,
	Filter: ethereum.FilterQuery{
		Addresses: []common.Address{token},
		Topics:    [][]common.Hash{{transferTopic}},
	},
	Store:        store,
	PollInterval: 12 * time.Second,
})
err = source.Run(ctx, exex.NewExExHandler(handler))
```

Use `NewSQLiteStore` for durable local chain state. Use `NewMemoryStore` for
tests and short-lived processes.

## Examples

Run the ERC20 transfer indexer example:

```sh
ETH_RPC_URL=http://localhost:8545 \
TOKEN_ADDRESS=0x0000000000000000000000000000000000000000 \
START_BLOCK=12345000 \
go run ./examples/erc20_transfers
```

The example implements an in-memory `ChainHandler` for
`Transfer(address,address,uint256)` logs. `RPCSource` owns RPC polling, log
fetching, and reorg detection; the handler only applies committed chain segments
and removes reverted ones.
