package indexer_test

import (
	"context"
	"log"
	"os"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/tamanoir/indexer"
)

var transferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

type TransferCounter struct {
	Count int
}

func (h *TransferCounter) Filter() ethereum.FilterQuery {
	return ethereum.FilterQuery{
		Topics: [][]common.Hash{{transferTopic}},
	}
}

func (h *TransferCounter) HandleLogs(_ context.Context, logs []types.Log) error {
	h.Count += len(logs)
	return nil
}

func Example() {
	rpcURL := os.Getenv("ETH_RPC_URL")
	if rpcURL == "" {
		return
	}

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	counter := &TransferCounter{}
	idx, err := indexer.New(client, counter,
		indexer.WithStartBlock(18_000_000),
		indexer.WithBatchSize(2_000),
		indexer.WithCheckpointInterval(10_000),
		indexer.WithCheckpointStore(indexer.FileCheckpoints(".cache/erc20-transfers")),
		indexer.WithLogCache(indexer.FileLogCache(".cache/erc20-transfers")),
	)
	if err != nil {
		log.Fatal(err)
	}

	head, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	if err := idx.SyncTo(context.Background(), head); err != nil {
		log.Fatal(err)
	}
}
