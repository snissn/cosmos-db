package db

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cast"
	"treedb"
	"treedb/caching"
)

// TreeDBBackend represents the TreeDB backend.
const TreeDBBackend BackendType = "treedb"

func init() {
	registerDBCreator(TreeDBBackend, func(name, dir string, opts Options) (DB, error) {
		return NewTreeDB(name, dir, opts)
	}, false)
}

// TreeDBWrapper wraps caching.DB to satisfy cosmosdb.DB interface.
type TreeDBWrapper struct {
	*caching.DB
}

// NewTreeDB creates a new TreeDB database at dir/name.db.
func NewTreeDB(name, dir string, opts Options) (DB, error) {
	dbPath := filepath.Join(dir, name+".db")

	keepRecent := uint64(10000)
	if opts != nil {
		if v := opts.Get("keep_recent"); v != nil {
			keepRecent = cast.ToUint64(v)
		}
	}

	tdbOpts := treedb.Options{
		Dir:            dbPath,
		KeepRecent:     keepRecent,
		EnableCaching:  true,
		FlushThreshold: 4 * 1024 * 1024,
	}

	db, err := treedb.OpenCached(tdbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open treedb cached: %w", err)
	}

	return &TreeDBWrapper{db}, nil
}

// Iterator returns a new iterator.
func (db *TreeDBWrapper) Iterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachingIteratorWrapper{it, start, end}, nil
}

// ReverseIterator returns a new reverse iterator.
func (db *TreeDBWrapper) ReverseIterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachingIteratorWrapper{it, start, end}, nil
}

// NewBatch returns a new batch.
func (db *TreeDBWrapper) NewBatch() Batch {
	return db.DB.NewBatch()
}

// NewBatchWithSize returns a new batch with a size hint.
func (db *TreeDBWrapper) NewBatchWithSize(size int) Batch {
	return db.DB.NewBatchWithSize(size)
}

// reusing cachingIteratorWrapper from treemapgemini.go? No, it's private there.
// I should define it here too or make it public/shared.
// Since they are in the same package 'db', I can reuse it IF it's in the same package.
// treemapgemini.go is package db. treedb.go is package db.
// So cachingIteratorWrapper IS visible.