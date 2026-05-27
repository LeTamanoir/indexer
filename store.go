package exex

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// StoredBlock is the chain state retained by an RPC source.
type StoredBlock struct {
	Number     uint64
	Hash       common.Hash
	ParentHash common.Hash
	Logs       []types.Log
}

// ChainStore stores observed chain state used to synthesize chain updates.
type ChainStore interface {
	Head(ctx context.Context) (StoredBlock, bool, error)
	BlockByHash(ctx context.Context, hash common.Hash) (StoredBlock, bool, error)
	UpdateCanonicalChain(ctx context.Context, reverted []StoredBlock, committed []StoredBlock) error
}

func cloneStoredBlock(block StoredBlock) StoredBlock {
	block.Logs = cloneLogs(block.Logs)
	return block
}

func cloneLogs(logs []types.Log) []types.Log {
	if len(logs) == 0 {
		return nil
	}

	out := make([]types.Log, len(logs))
	copy(out, logs)
	for i := range out {
		out[i].Topics = append([]common.Hash(nil), logs[i].Topics...)
		out[i].Data = append([]byte(nil), logs[i].Data...)
	}
	return out
}
