package db

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cast"
	"treedb"
)

// TreeDBBackend represents the TreeDB backend.
const TreeDBBackend BackendType = "treedb"

func init() {
	registerDBCreator(TreeDBBackend, func(name, dir string, opts Options) (DB, error) {
		return NewTreeDB(name, dir, opts)
	}, false)
}

// TreeDBWrapper wraps treedb.DB to satisfy cosmosdb.DB interface.
type TreeDBWrapper struct {
	*treedb.DB
}

// NewTreeDB creates a new TreeDB database at dir/name.db.
func NewTreeDB(name, dir string, opts Options) (DB, error) {
	dbPath := filepath.Join(dir, name+".db")

	keepRecent := uint64(0) // Default to 0 (aggressive pruning) as it performed better in load tests
	if opts != nil {
		if v := opts.Get("keep_recent"); v != nil {
			keepRecent = cast.ToUint64(v)
		}
	}

	tdbOpts := treedb.Options{
		Dir:        dbPath,
		KeepRecent: keepRecent,
	}

	db, err := treedb.Open(tdbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open treedb: %w", err)
	}

	return &TreeDBWrapper{db}, nil
}

// NewBatch returns a new batch.
func (db *TreeDBWrapper) NewBatch() Batch {
	return db.DB.NewBatch()
}

// NewBatchWithSize returns a new batch with a size hint.
func (db *TreeDBWrapper) NewBatchWithSize(size int) Batch {
	return db.DB.NewBatchWithSize(size)
}

// Iterator returns a new iterator.
func (db *TreeDBWrapper) Iterator(start, end []byte) (Iterator, error) {
	return db.DB.Iterator(start, end)
}

// ReverseIterator returns a new reverse iterator.
func (db *TreeDBWrapper) ReverseIterator(start, end []byte) (Iterator, error) {
	return db.DB.ReverseIterator(start, end)
}