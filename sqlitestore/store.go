// Package sqlitestore provides a SQLite-backed exex.ChainStore.
package sqlitestore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	exex "github.com/letamanoir/go-exex"
	_ "modernc.org/sqlite"
)

const driverName = "sqlite"

// Store stores chain state in a local SQLite database.
type Store struct {
	db *sql.DB
}

// New opens or creates a SQLite-backed chain store at path.
func New(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("sqlitestore: path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create sqlite store directory: %w", err)
		}
	}

	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &Store{db: db}
	if err := store.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// Close closes the database opened by New.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Head(ctx context.Context) (exex.StoredBlock, bool, error) {
	var hashBytes []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key = 'head'`).Scan(&hashBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return exex.StoredBlock{}, false, nil
	}
	if err != nil {
		return exex.StoredBlock{}, false, fmt.Errorf("read sqlite head: %w", err)
	}

	hash, err := hashFromBytes(hashBytes)
	if err != nil {
		return exex.StoredBlock{}, false, fmt.Errorf("read sqlite head hash: %w", err)
	}

	block, ok, err := s.BlockByHash(ctx, hash)
	if err != nil {
		return exex.StoredBlock{}, false, err
	}
	if !ok {
		return exex.StoredBlock{}, false, fmt.Errorf("sqlite head block %s is missing", hash)
	}
	return block, true, nil
}

func (s *Store) BlockByHash(ctx context.Context, hash common.Hash) (exex.StoredBlock, bool, error) {
	block, err := scanBlock(s.db.QueryRowContext(ctx, `
		SELECT hash, parent_hash, number, logs
		FROM blocks
		WHERE hash = ?
	`, hash.Bytes()))
	if errors.Is(err, sql.ErrNoRows) {
		return exex.StoredBlock{}, false, nil
	}
	if err != nil {
		return exex.StoredBlock{}, false, err
	}
	return block, true, nil
}

func (s *Store) UpdateCanonicalChain(ctx context.Context, reverted []exex.StoredBlock, committed []exex.StoredBlock) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite chain update: %w", err)
	}
	defer rollbackUnlessCommitted(tx)

	for _, block := range committed {
		if err := putBlock(ctx, tx, block); err != nil {
			return err
		}
	}

	switch {
	case len(committed) > 0:
		if err := setHead(ctx, tx, committed[len(committed)-1].Hash); err != nil {
			return err
		}
	case len(reverted) > 0:
		parentHash := reverted[0].ParentHash
		if parentHash == (common.Hash{}) {
			if _, err := tx.ExecContext(ctx, `DELETE FROM metadata WHERE key = 'head'`); err != nil {
				return fmt.Errorf("clear sqlite head: %w", err)
			}
			break
		}
		if err := setHead(ctx, tx, parentHash); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite chain update: %w", err)
	}
	return nil
}

func (s *Store) init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		return fmt.Errorf("configure sqlite busy timeout: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return fmt.Errorf("configure sqlite foreign keys: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize sqlite store: %w", err)
	}
	return nil
}

const schema = `
CREATE TABLE IF NOT EXISTS blocks (
	hash BLOB PRIMARY KEY,
	parent_hash BLOB NOT NULL,
	number INTEGER NOT NULL,
	logs BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata (
	key TEXT PRIMARY KEY,
	value BLOB NOT NULL
);
`

func putBlock(ctx context.Context, tx *sql.Tx, block exex.StoredBlock) error {
	logs, err := encodeLogs(block.Logs)
	if err != nil {
		return fmt.Errorf("encode logs for block %s: %w", block.Hash, err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO blocks (hash, parent_hash, number, logs)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(hash) DO UPDATE SET
			parent_hash = excluded.parent_hash,
			number = excluded.number,
			logs = excluded.logs
	`, block.Hash.Bytes(), block.ParentHash.Bytes(), int64(block.Number), logs); err != nil {
		return fmt.Errorf("write sqlite block %d %s: %w", block.Number, block.Hash, err)
	}
	return nil
}

func setHead(ctx context.Context, tx *sql.Tx, hash common.Hash) error {
	if err := ensureBlockExists(ctx, tx, hash); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO metadata (key, value)
		VALUES ('head', ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, hash.Bytes()); err != nil {
		return fmt.Errorf("write sqlite head: %w", err)
	}
	return nil
}

func ensureBlockExists(ctx context.Context, tx *sql.Tx, hash common.Hash) error {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM blocks WHERE hash = ?`, hash.Bytes()).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlite block %s is missing", hash)
	}
	if err != nil {
		return fmt.Errorf("read sqlite block %s: %w", hash, err)
	}
	return nil
}

func scanBlock(row *sql.Row) (exex.StoredBlock, error) {
	var hashBytes []byte
	var parentHashBytes []byte
	var number int64
	var logsData []byte

	if err := row.Scan(&hashBytes, &parentHashBytes, &number, &logsData); err != nil {
		return exex.StoredBlock{}, err
	}
	if number < 0 {
		return exex.StoredBlock{}, fmt.Errorf("sqlite block number is negative: %d", number)
	}

	hash, err := hashFromBytes(hashBytes)
	if err != nil {
		return exex.StoredBlock{}, fmt.Errorf("read sqlite block hash: %w", err)
	}
	parentHash, err := hashFromBytes(parentHashBytes)
	if err != nil {
		return exex.StoredBlock{}, fmt.Errorf("read sqlite parent hash: %w", err)
	}
	logs, err := decodeLogs(logsData)
	if err != nil {
		return exex.StoredBlock{}, fmt.Errorf("decode sqlite logs for block %s: %w", hash, err)
	}

	return exex.StoredBlock{
		Number:     uint64(number),
		Hash:       hash,
		ParentHash: parentHash,
		Logs:       logs,
	}, nil
}

func hashFromBytes(data []byte) (common.Hash, error) {
	if len(data) != common.HashLength {
		return common.Hash{}, fmt.Errorf("got %d bytes, want %d", len(data), common.HashLength)
	}
	return common.BytesToHash(data), nil
}

func encodeLogs(logs []types.Log) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cloneLogs(logs)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeLogs(data []byte) ([]types.Log, error) {
	var logs []types.Log
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&logs); err != nil {
		return nil, err
	}
	return cloneLogs(logs), nil
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

func rollbackUnlessCommitted(tx *sql.Tx) {
	_ = tx.Rollback()
}
