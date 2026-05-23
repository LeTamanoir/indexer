package exex

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
	_ "modernc.org/sqlite"
)

const sqliteDriverName = "sqlite"

// SQLiteStore stores chain state in a local SQLite database.
type SQLiteStore struct {
	db      *sql.DB
	closeDB bool
}

// NewSQLiteStore opens or creates a SQLite-backed chain store at path.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		return nil, errors.New("exex: sqlite store path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create sqlite store directory: %w", err)
		}
	}

	db, err := sql.Open(sqliteDriverName, path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{db: db, closeDB: true}
	if err := store.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// NewSQLiteStoreFromDB creates a chain store using an existing database handle.
func NewSQLiteStoreFromDB(db *sql.DB) (*SQLiteStore, error) {
	if db == nil {
		return nil, errors.New("exex: nil sqlite database")
	}

	store := &SQLiteStore{db: db}
	if err := store.init(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

// Close closes the database opened by NewSQLiteStore.
func (s *SQLiteStore) Close() error {
	if s == nil || !s.closeDB {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) Head(ctx context.Context) (StoredBlock, bool, error) {
	var hashBytes []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key = 'head'`).Scan(&hashBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredBlock{}, false, nil
	}
	if err != nil {
		return StoredBlock{}, false, fmt.Errorf("read sqlite head: %w", err)
	}

	hash, err := hashFromBytes(hashBytes)
	if err != nil {
		return StoredBlock{}, false, fmt.Errorf("read sqlite head hash: %w", err)
	}

	block, ok, err := s.BlockByHash(ctx, hash)
	if err != nil {
		return StoredBlock{}, false, err
	}
	if !ok {
		return StoredBlock{}, false, fmt.Errorf("sqlite head block %s is missing", hash)
	}
	return block, true, nil
}

func (s *SQLiteStore) BlockByHash(ctx context.Context, hash common.Hash) (StoredBlock, bool, error) {
	block, err := scanSQLiteBlock(s.db.QueryRowContext(ctx, `
		SELECT hash, parent_hash, number, logs
		FROM blocks
		WHERE hash = ?
	`, hash.Bytes()))
	if errors.Is(err, sql.ErrNoRows) {
		return StoredBlock{}, false, nil
	}
	if err != nil {
		return StoredBlock{}, false, err
	}
	return block, true, nil
}

func (s *SQLiteStore) CanonicalBlock(ctx context.Context, number uint64) (StoredBlock, bool, error) {
	block, err := scanSQLiteBlock(s.db.QueryRowContext(ctx, `
		SELECT b.hash, b.parent_hash, b.number, b.logs
		FROM canonical_blocks c
		JOIN blocks b ON b.hash = c.hash
		WHERE c.number = ?
	`, int64(number)))
	if errors.Is(err, sql.ErrNoRows) {
		return StoredBlock{}, false, nil
	}
	if err != nil {
		return StoredBlock{}, false, err
	}
	return block, true, nil
}

func (s *SQLiteStore) UpdateCanonicalChain(ctx context.Context, reverted []StoredBlock, committed []StoredBlock) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite chain update: %w", err)
	}
	defer rollbackUnlessCommitted(tx)

	for _, block := range reverted {
		if _, err := tx.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE number = ?`, int64(block.Number)); err != nil {
			return fmt.Errorf("delete sqlite canonical block %d: %w", block.Number, err)
		}
	}

	for _, block := range committed {
		if err := putSQLiteBlock(ctx, tx, block); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO canonical_blocks (number, hash)
			VALUES (?, ?)
			ON CONFLICT(number) DO UPDATE SET hash = excluded.hash
		`, int64(block.Number), block.Hash.Bytes()); err != nil {
			return fmt.Errorf("write sqlite canonical block %d: %w", block.Number, err)
		}
	}

	switch {
	case len(committed) > 0:
		if err := setSQLiteHead(ctx, tx, committed[len(committed)-1].Hash); err != nil {
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
		if err := ensureSQLiteBlockExists(ctx, tx, parentHash); err != nil {
			return err
		}
		if err := setSQLiteHead(ctx, tx, parentHash); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite chain update: %w", err)
	}
	return nil
}

func (s *SQLiteStore) init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		return fmt.Errorf("configure sqlite busy timeout: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return fmt.Errorf("configure sqlite foreign keys: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, sqliteSchema); err != nil {
		return fmt.Errorf("initialize sqlite store: %w", err)
	}
	return nil
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS blocks (
	hash BLOB PRIMARY KEY,
	parent_hash BLOB NOT NULL,
	number INTEGER NOT NULL,
	logs BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_blocks (
	number INTEGER PRIMARY KEY,
	hash BLOB NOT NULL REFERENCES blocks(hash)
);

CREATE TABLE IF NOT EXISTS metadata (
	key TEXT PRIMARY KEY,
	value BLOB NOT NULL
);
`

func putSQLiteBlock(ctx context.Context, tx *sql.Tx, block StoredBlock) error {
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

func setSQLiteHead(ctx context.Context, tx *sql.Tx, hash common.Hash) error {
	if err := ensureSQLiteBlockExists(ctx, tx, hash); err != nil {
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

func ensureSQLiteBlockExists(ctx context.Context, tx *sql.Tx, hash common.Hash) error {
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

func scanSQLiteBlock(row *sql.Row) (StoredBlock, error) {
	var hashBytes []byte
	var parentHashBytes []byte
	var number int64
	var logsData []byte

	if err := row.Scan(&hashBytes, &parentHashBytes, &number, &logsData); err != nil {
		return StoredBlock{}, err
	}
	if number < 0 {
		return StoredBlock{}, fmt.Errorf("sqlite block number is negative: %d", number)
	}

	hash, err := hashFromBytes(hashBytes)
	if err != nil {
		return StoredBlock{}, fmt.Errorf("read sqlite block hash: %w", err)
	}
	parentHash, err := hashFromBytes(parentHashBytes)
	if err != nil {
		return StoredBlock{}, fmt.Errorf("read sqlite parent hash: %w", err)
	}
	logs, err := decodeLogs(logsData)
	if err != nil {
		return StoredBlock{}, fmt.Errorf("decode sqlite logs for block %s: %w", hash, err)
	}

	return StoredBlock{
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

func rollbackUnlessCommitted(tx *sql.Tx) {
	_ = tx.Rollback()
}
