// Package main demonstrates an RPC-backed ExEx indexer for ERC20 Transfer logs.
package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/letamanoir/go-exex"
)

var erc20TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

type ERC20Transfer struct {
	Token       common.Address
	From        common.Address
	To          common.Address
	Value       *big.Int
	BlockNumber uint64
	BlockHash   common.Hash
	TxHash      common.Hash
	LogIndex    uint
}

type logKey struct {
	BlockHash common.Hash
	TxHash    common.Hash
	LogIndex  uint
}

type ERC20TransferIndexer struct {
	transfers map[logKey]ERC20Transfer
}

func NewERC20TransferIndexer() *ERC20TransferIndexer {
	return &ERC20TransferIndexer{
		transfers: make(map[logKey]ERC20Transfer),
	}
}

func (i *ERC20TransferIndexer) Commit(ctx context.Context, chain exex.Chain) error {
	if err := chain.ForEachLog(func(log types.Log) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		transfer, err := decodeERC20Transfer(log)
		if err != nil {
			return err
		}
		i.transfers[transfer.key()] = transfer
		return nil
	}); err != nil {
		return err
	}

	if chain.LogCount() > 0 {
		fmt.Printf("committed blocks=%d..%d logs=%d transfers=%d\n", chain.FromBlock, chain.ToBlock, chain.LogCount(), i.Count())
	}
	return nil
}

func (i *ERC20TransferIndexer) Revert(ctx context.Context, chain exex.Chain) error {
	if err := chain.ForEachLog(func(log types.Log) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		transfer, err := decodeERC20Transfer(log)
		if err != nil {
			return err
		}
		delete(i.transfers, transfer.key())
		return nil
	}); err != nil {
		return err
	}

	if chain.LogCount() > 0 {
		fmt.Printf("reverted blocks=%d..%d logs=%d transfers=%d\n", chain.FromBlock, chain.ToBlock, chain.LogCount(), i.Count())
	}
	return nil
}

func (i *ERC20TransferIndexer) Count() int {
	return len(i.transfers)
}

func (t ERC20Transfer) key() logKey {
	return logKey{
		BlockHash: t.BlockHash,
		TxHash:    t.TxHash,
		LogIndex:  t.LogIndex,
	}
}

func decodeERC20Transfer(log types.Log) (ERC20Transfer, error) {
	if len(log.Topics) != 3 || log.Topics[0] != erc20TransferTopic {
		return ERC20Transfer{}, errors.New("decode ERC20 transfer: log does not match Transfer(address,address,uint256)")
	}
	if len(log.Data) != common.HashLength {
		return ERC20Transfer{}, fmt.Errorf("decode ERC20 transfer: value has %d bytes, want %d", len(log.Data), common.HashLength)
	}

	return ERC20Transfer{
		Token:       log.Address,
		From:        common.BytesToAddress(log.Topics[1].Bytes()),
		To:          common.BytesToAddress(log.Topics[2].Bytes()),
		Value:       new(big.Int).SetBytes(log.Data),
		BlockNumber: log.BlockNumber,
		BlockHash:   log.BlockHash,
		TxHash:      log.TxHash,
		LogIndex:    log.Index,
	}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	rpcURL := os.Getenv("ETH_RPC_URL")
	if rpcURL == "" {
		return errors.New("ETH_RPC_URL is required")
	}

	tokenValue := os.Getenv("TOKEN_ADDRESS")
	if !common.IsHexAddress(tokenValue) {
		return errors.New("TOKEN_ADDRESS must be a hex Ethereum address")
	}
	token := common.HexToAddress(tokenValue)

	start, startLabel, err := parseStart(os.Getenv("START_BLOCK"))
	if err != nil {
		return err
	}

	pollInterval, err := parsePollInterval(os.Getenv("POLL_INTERVAL"))
	if err != nil {
		return err
	}

	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("dial ethereum rpc: %w", err)
	}
	defer client.Close()

	indexer := NewERC20TransferIndexer()

	source, err := exex.NewRPCSource(client, exex.RPCSourceConfig{
		Start: start,
		Filter: ethereum.FilterQuery{
			Addresses: []common.Address{token},
			Topics:    [][]common.Hash{{erc20TransferTopic}},
		},
		Store:        exex.NewMemoryStore(),
		PollInterval: pollInterval,
	})
	if err != nil {
		return err
	}

	fmt.Printf("indexing ERC20 transfers token=%s start=%s\n", token.Hex(), startLabel)
	return source.Run(ctx, indexer)
}

func parseStart(raw string) (exex.RPCStart, string, error) {
	if raw == "" || raw == "latest" {
		return exex.StartAtLatest(), "latest", nil
	}
	block, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return exex.RPCStart{}, "", fmt.Errorf("START_BLOCK must be a uint64 or latest: %w", err)
	}
	return exex.StartAtBlock(block), strconv.FormatUint(block, 10), nil
}

func parsePollInterval(raw string) (time.Duration, error) {
	if raw == "" {
		return 12 * time.Second, nil
	}
	if raw == "0" {
		return 0, nil
	}
	interval, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("POLL_INTERVAL must be a duration such as 12s or 1m: %w", err)
	}
	return interval, nil
}
