package db

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cast"
	"treedb"
	"treedb/caching"
)

// TreeMapGeminiBackend represents the TreeMapGemini backend.
const TreeMapGeminiBackend BackendType = "treemapgemini"

func init() {
	registerDBCreator(TreeMapGeminiBackend, func(name, dir string, opts Options) (DB, error) {
		return NewTreeMapGeminiDB(name, dir, opts)
	}, false)
}

// TreeMapGeminiWrapper wraps caching.DB to satisfy cosmosdb.DB interface.
type TreeMapGeminiWrapper struct {
	*caching.DB
}

// NewTreeMapGeminiDB creates a new TreeMapGemini database at dir/name.db.
func NewTreeMapGeminiDB(name, dir string, opts Options) (DB, error) {
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

	return &TreeMapGeminiWrapper{db}, nil
}

// Iterator returns a new iterator.
func (db *TreeMapGeminiWrapper) Iterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachingIteratorWrapper{it, start, end}, nil
}

// ReverseIterator returns a new reverse iterator.
func (db *TreeMapGeminiWrapper) ReverseIterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachingIteratorWrapper{it, start, end}, nil
}
// NewBatch returns a new batch.
func (db *TreeMapGeminiWrapper) NewBatch() Batch {
	return db.DB.NewBatch()
}

// NewBatchWithSize returns a new batch with a size hint.
func (db *TreeMapGeminiWrapper) NewBatchWithSize(size int) Batch {
	return db.DB.NewBatchWithSize(size)
}

type cachingIteratorWrapper struct {
	it    interface {
		Next()
		Valid() bool
		Key() []byte
		Value() []byte
		Close() error
		Error() error
	}
	start, end []byte
}

func (w *cachingIteratorWrapper) Domain() ([]byte, []byte) { return w.start, w.end }
func (w *cachingIteratorWrapper) Valid() bool              { return w.it.Valid() }
func (w *cachingIteratorWrapper) Next()                    { w.it.Next() }
func (w *cachingIteratorWrapper) Key() []byte              { return w.it.Key() }
func (w *cachingIteratorWrapper) Value() []byte              { return w.it.Value() }
func (w *cachingIteratorWrapper) Error() error             { return w.it.Error() }
func (w *cachingIteratorWrapper) Close() error             { return w.it.Close() }
