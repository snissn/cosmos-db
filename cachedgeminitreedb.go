package db

import (
	"fmt"
	"os"
	"path/filepath"

	geminidb "github.com/snissn/gomap/TreeDB"
	geminicaching "github.com/snissn/gomap/TreeDB/caching"
	"github.com/spf13/cast"
)

// CachedGeminiTreeDBBackend represents the CachedGeminiTreeDB backend.
const CachedGeminiTreeDBBackend BackendType = "geminicached"

func init() {
	registerDBCreator(CachedGeminiTreeDBBackend, func(name, dir string, opts Options) (DB, error) {
		return NewCachedGeminiTreeDB(name, dir, opts)
	}, false)
}

// CachedGeminiTreeDBWrapper wraps geminicaching.DB to satisfy cosmosdb.DB interface.
type CachedGeminiTreeDBWrapper struct {
	*geminicaching.DB
}

// NewCachedGeminiTreeDB creates a new CachedGeminiTreeDB database at dir/name.db.
func NewCachedGeminiTreeDB(name, dir string, opts Options) (DB, error) {
	dbPath := filepath.Join(dir, name+".db")

	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db dir: %w", err)
	}

	keepRecent := uint64(10000)
	if opts != nil {
		if v := opts.Get("keep_recent"); v != nil {
			keepRecent = cast.ToUint64(v)
		}
	}

	tdbOpts := geminidb.Options{
		Dir:        dbPath,
		ChunkSize:  64 * 1024 * 1024,
		KeepRecent: keepRecent,
	}

	backendDB, err := geminidb.Open(tdbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open backend db: %w", err)
	}

	flushThreshold := int64(4 * 1024 * 1024)
	if opts != nil {
		if v := opts.Get("flush_threshold"); v != nil {
			flushThreshold = cast.ToInt64(v)
		}
	}

	db, err := geminicaching.Open(dbPath, backendDB, flushThreshold)
	if err != nil {
		backendDB.Close()
		return nil, fmt.Errorf("failed to open cached db: %w", err)
	}

	return &CachedGeminiTreeDBWrapper{db}, nil
}

// Iterator returns a new iterator.
func (db *CachedGeminiTreeDBWrapper) Iterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachedGeminiIteratorWrapper{it, start, end}, nil
}

// ReverseIterator returns a new reverse iterator.
func (db *CachedGeminiTreeDBWrapper) ReverseIterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachedGeminiIteratorWrapper{it, start, end}, nil
}

// NewBatch returns a new batch.
func (db *CachedGeminiTreeDBWrapper) NewBatch() Batch {
	return db.DB.NewBatch()
}

// NewBatchWithSize returns a new batch with a size hint.
func (db *CachedGeminiTreeDBWrapper) NewBatchWithSize(size int) Batch {
	return db.DB.NewBatchWithSize(size)
}

// Print delegates to the underlying geminicaching.DB.
func (db *CachedGeminiTreeDBWrapper) Print() error {
	return db.DB.Print()
}

type cachedGeminiIteratorWrapper struct {
	it interface {
		Next()
		Valid() bool
		Key() []byte
		Value() []byte
		Close() error
		Error() error
	}
	start, end []byte
}

func (w *cachedGeminiIteratorWrapper) Domain() ([]byte, []byte) { return w.start, w.end }
func (w *cachedGeminiIteratorWrapper) Valid() bool              { return w.it.Valid() }

func (w *cachedGeminiIteratorWrapper) Next() {
	if !w.Valid() {
		panic("Next called on invalid iterator")
	}
	w.it.Next()
}

func (w *cachedGeminiIteratorWrapper) Key() []byte {
	if !w.Valid() {
		panic("Key called on invalid iterator")
	}
	return w.it.Key()
}

func (w *cachedGeminiIteratorWrapper) Value() []byte {
	if !w.Valid() {
		panic("Value called on invalid iterator")
	}
	return w.it.Value()
}

func (w *cachedGeminiIteratorWrapper) Error() error { return w.it.Error() }
func (w *cachedGeminiIteratorWrapper) Close() error { return w.it.Close() }
