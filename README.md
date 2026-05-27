# go-exex

`go-exex` defines ExEx-style primitives for Ethereum indexers.

```go
type Handler interface {
	Commit(ctx context.Context, chain exex.Chain) error
	Revert(ctx context.Context, chain exex.Chain) error
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
store := exex.NewMemoryStore()
source, err := exex.NewRPCSource(client, exex.RPCSourceConfig{
	Start: exex.StartAtBlock(12_345_000),
	Filter: ethereum.FilterQuery{
		Addresses: []common.Address{token},
		Topics:    [][]common.Hash{{transferTopic}},
	},
	Store:        store,
	PollInterval: 12 * time.Second,
})
err = source.Run(ctx, handler)
```

Use `NewMemoryStore` for tests and short-lived processes. Use
`sqlitestore.New` from `github.com/letamanoir/go-exex/sqlitestore` for durable
local chain state. When `Start` is omitted, the source starts at the current RPC
head and only processes future blocks; use `StartAtBlock(0)` to backfill from
genesis.

## Examples

Run the ERC20 transfer indexer example:

```sh
ETH_RPC_URL=http://localhost:8545 \
TOKEN_ADDRESS=0x0000000000000000000000000000000000000000 \
go run ./examples/erc20_transfers
```

Set `START_BLOCK=12345000` to backfill from a specific block. The example
implements an in-memory `Handler` for `Transfer(address,address,uint256)` logs.
`RPCSource` owns RPC polling, log fetching, and reorg detection; the handler only
applies committed chain segments and removes reverted ones.
