package indexer

import (
	"cmp"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
)

// CheckpointStore persists handler state at selected blocks. Load receives the
// current handler as a prototype and returns a restored handler state.
type CheckpointStore interface {
	Save(ctx context.Context, block uint64, state any) error
	Load(ctx context.Context, target uint64, current any) (block uint64, state any, ok bool, err error)
	Prune(ctx context.Context, from uint64) error
}

// NoCheckpoints disables checkpointing.
func NoCheckpoints() CheckpointStore {
	return noCheckpointStore{}
}

type disabledCheckpointStore interface {
	disabledCheckpointStore()
}

type noCheckpointStore struct{}

func (noCheckpointStore) Save(context.Context, uint64, any) error {
	return nil
}

func (noCheckpointStore) Load(context.Context, uint64, any) (uint64, any, bool, error) {
	return 0, nil, false, nil
}

func (noCheckpointStore) Prune(context.Context, uint64) error {
	return nil
}

func (noCheckpointStore) String() string {
	return "none"
}

func (noCheckpointStore) disabledCheckpointStore() {}

// FileCheckpoints stores checkpoints as gob files in path.
func FileCheckpoints(path string) CheckpointStore {
	return fileCheckpointStore{path: path}
}

type fileCheckpointStore struct {
	path string
}

func (s fileCheckpointStore) Save(_ context.Context, block uint64, state any) error {
	if err := os.MkdirAll(s.path, 0755); err != nil {
		return err
	}

	file := s.checkpointFile(block)
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(state)
}

func (s fileCheckpointStore) Load(_ context.Context, target uint64, current any) (uint64, any, bool, error) {
	blocks, err := s.checkpointBlocks()
	if err != nil {
		return 0, nil, false, err
	}
	if len(blocks) == 0 {
		return 0, nil, false, nil
	}

	idx, ok := slices.BinarySearchFunc(blocks, target, func(block uint64, target uint64) int {
		return cmp.Compare(block, target)
	})
	if !ok {
		idx--
	}
	if idx < 0 {
		return 0, nil, false, nil
	}

	block := blocks[idx]
	f, err := os.Open(s.checkpointFile(block))
	if err != nil {
		return 0, nil, false, err
	}
	defer f.Close()

	decodeTarget, restoredState, err := newCheckpointState(current)
	if err != nil {
		return 0, nil, false, err
	}

	if err := gob.NewDecoder(f).Decode(decodeTarget); err != nil {
		return 0, nil, false, err
	}

	return block, restoredState(), true, nil
}

func (s fileCheckpointStore) Prune(_ context.Context, from uint64) error {
	blocks, err := s.checkpointBlocks()
	if err != nil {
		return err
	}
	for _, block := range blocks {
		if block < from {
			continue
		}
		if err := os.Remove(s.checkpointFile(block)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (s fileCheckpointStore) String() string {
	return s.path
}

func (s fileCheckpointStore) checkpointFile(block uint64) string {
	return filepath.Join(s.path, fmt.Sprintf("checkpoint-%d.gob", block))
}

func (s fileCheckpointStore) checkpointBlocks() ([]uint64, error) {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	blocks := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "checkpoint-") || !strings.HasSuffix(name, ".gob") {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(name, "checkpoint-"), ".gob")
		block, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			continue
		}
		blocks = append(blocks, block)
	}
	slices.Sort(blocks)
	return blocks, nil
}

// LogCache persists fetched logs for immutable-enough historical ranges.
type LogCache interface {
	Load(ctx context.Context, from, to uint64) ([]types.Log, bool, error)
	Save(ctx context.Context, from, to uint64, logs []types.Log) error
}

// NoLogCache disables log caching.
func NoLogCache() LogCache {
	return noLogCache{}
}

type noLogCache struct{}

func (noLogCache) Load(context.Context, uint64, uint64) ([]types.Log, bool, error) {
	return nil, false, nil
}

func (noLogCache) Save(context.Context, uint64, uint64, []types.Log) error {
	return nil
}

func (noLogCache) String() string {
	return "none"
}

// FileLogCache stores log batches as gob files in path.
func FileLogCache(path string) LogCache {
	return fileLogCache{path: path}
}

type fileLogCache struct {
	path string
}

func (c fileLogCache) Load(_ context.Context, from, to uint64) ([]types.Log, bool, error) {
	f, err := os.Open(c.logFile(from, to))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer f.Close()

	var logs []types.Log
	if err := gob.NewDecoder(f).Decode(&logs); err != nil {
		return nil, false, err
	}
	return logs, true, nil
}

func (c fileLogCache) Save(_ context.Context, from, to uint64, logs []types.Log) error {
	if err := os.MkdirAll(c.path, 0755); err != nil {
		return err
	}

	f, err := os.Create(c.logFile(from, to))
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(logs)
}

func (c fileLogCache) String() string {
	return c.path
}

func (c fileLogCache) logFile(from, to uint64) string {
	return filepath.Join(c.path, fmt.Sprintf("logs-%d-%d.gob", from, to))
}

func describeStore(store any) string {
	if store == nil {
		return ""
	}
	if s, ok := store.(fmt.Stringer); ok {
		return s.String()
	}
	return strings.TrimPrefix(fmt.Sprintf("%T", store), "*")
}

func newCheckpointState(current any) (decodeTarget any, restoredState func() any, err error) {
	typ := reflect.TypeOf(current)
	if typ == nil {
		return nil, nil, fmt.Errorf("checkpoint state type is nil")
	}

	if typ.Kind() == reflect.Pointer {
		value := reflect.New(typ.Elem())
		return value.Interface(), func() any { return value.Interface() }, nil
	}

	value := reflect.New(typ)
	return value.Interface(), func() any { return value.Elem().Interface() }, nil
}
