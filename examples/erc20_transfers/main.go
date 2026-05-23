// Package main demonstrates an RPC-backed ExEx indexer for ERC20 Transfer logs.
package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"sort"
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
	token     common.Address
	transfers map[logKey]ERC20Transfer
}

func NewERC20TransferIndexer(token common.Address) *ERC20TransferIndexer {
	return &ERC20TransferIndexer{
		token:     token,
		transfers: make(map[logKey]ERC20Transfer),
	}
}

func (i *ERC20TransferIndexer) CommitChain(ctx context.Context, chain exex.Chain) error {
	return chain.ForEachLog(ctx, func(_ exex.BlockLogs, log types.Log) error {
		transfer, ok, err := decodeERC20Transfer(log)
		if err != nil || !ok || transfer.Token != i.token {
			return err
		}

		i.transfers[transfer.key()] = transfer
		return nil
	})
}

func (i *ERC20TransferIndexer) RevertChain(ctx context.Context, chain exex.Chain) error {
	return chain.ForEachLog(ctx, func(_ exex.BlockLogs, log types.Log) error {
		transfer, ok, err := decodeERC20Transfer(log)
		if err != nil || !ok || transfer.Token != i.token {
			return err
		}

		delete(i.transfers, transfer.key())
		return nil
	})
}

func (i *ERC20TransferIndexer) Transfers() []ERC20Transfer {
	transfers := make([]ERC20Transfer, 0, len(i.transfers))
	for _, transfer := range i.transfers {
		transfers = append(transfers, transfer)
	}

	sort.Slice(transfers, func(a, b int) bool {
		if transfers[a].BlockNumber != transfers[b].BlockNumber {
			return transfers[a].BlockNumber < transfers[b].BlockNumber
		}
		return transfers[a].LogIndex < transfers[b].LogIndex
	})

	return transfers
}

func (t ERC20Transfer) key() logKey {
	return logKey{
		BlockHash: t.BlockHash,
		TxHash:    t.TxHash,
		LogIndex:  t.LogIndex,
	}
}

func decodeERC20Transfer(log types.Log) (ERC20Transfer, bool, error) {
	if len(log.Topics) != 3 || log.Topics[0] != erc20TransferTopic {
		return ERC20Transfer{}, false, nil
	}
	if len(log.Data) != common.HashLength {
		return ERC20Transfer{}, false, fmt.Errorf("decode ERC20 transfer: value has %d bytes, want %d", len(log.Data), common.HashLength)
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
	}, true, nil
}

type ReportingHandler struct {
	indexer *ERC20TransferIndexer
}

func (h ReportingHandler) HandleNotification(ctx context.Context, notification exex.ExExNotification) error {
	if err := notification.Apply(ctx, h.indexer); err != nil {
		return err
	}

	if notification.LogCount() > 0 || notification.Kind == exex.ChainReorged {
		fmt.Printf("processed %s logs=%d transfers=%d\n", notification.Kind, notification.LogCount(), len(h.indexer.Transfers()))
	}
	return nil
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

	startBlock, err := parseStartBlock(os.Getenv("START_BLOCK"))
	if err != nil {
		return err
	}

	pollInterval, err := parsePollInterval(os.Getenv("POLL_INTERVAL"))
	if err != nil {
		return err
	}

	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("dial Ethereum RPC: %w", err)
	}
	defer client.Close()

	indexer := NewERC20TransferIndexer(token)

	source, err := exex.NewRPCSource(client, exex.RPCSourceConfig{
		StartBlock: startBlock,
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

	fmt.Printf("indexing ERC20 transfers token=%s start=%d\n", token.Hex(), startBlock)
	return source.Run(ctx, ReportingHandler{indexer: indexer})
}

func parseStartBlock(raw string) (uint64, error) {
	if raw == "" {
		return 0, nil
	}
	block, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("START_BLOCK must be a uint64: %w", err)
	}
	return block, nil
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
