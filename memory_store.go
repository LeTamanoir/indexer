package exex

import (
	"context"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// MemoryStore keeps chain state in memory. It is useful for tests and short-lived
// processes, but it cannot resume after restart.
type MemoryStore struct {
	mu      sync.RWMutex
	byHash  map[common.Hash]StoredBlock
	head    common.Hash
	hasHead bool
}

// NewMemoryStore creates an empty in-memory chain store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		byHash: make(map[common.Hash]StoredBlock),
	}
}

func (s *MemoryStore) Head(ctx context.Context) (StoredBlock, bool, error) {
	if err := ctx.Err(); err != nil {
		return StoredBlock{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.hasHead {
		return StoredBlock{}, false, nil
	}

	block, ok := s.byHash[s.head]
	if !ok {
		return StoredBlock{}, false, fmt.Errorf("memory head block %s is missing", s.head)
	}
	return cloneStoredBlock(block), true, nil
}

func (s *MemoryStore) BlockByHash(ctx context.Context, hash common.Hash) (StoredBlock, bool, error) {
	if err := ctx.Err(); err != nil {
		return StoredBlock{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	block, ok := s.byHash[hash]
	if !ok {
		return StoredBlock{}, false, nil
	}
	return cloneStoredBlock(block), true, nil
}

func (s *MemoryStore) UpdateCanonicalChain(ctx context.Context, reverted []StoredBlock, committed []StoredBlock) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, block := range committed {
		block = cloneStoredBlock(block)
		s.byHash[block.Hash] = block
	}

	switch {
	case len(committed) > 0:
		s.head = committed[len(committed)-1].Hash
		s.hasHead = true
	case len(reverted) > 0:
		parentHash := reverted[0].ParentHash
		if parentHash == (common.Hash{}) {
			s.head = common.Hash{}
			s.hasHead = false
			return nil
		}
		if _, ok := s.byHash[parentHash]; !ok {
			return fmt.Errorf("memory block %s is missing", parentHash)
		}
		s.head = parentHash
		s.hasHead = true
	}

	return nil
}
