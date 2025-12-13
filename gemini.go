package db

import (
	"fmt"
	"path/filepath"

	geminicaching "github.com/snissn/gomap-gemini/TreeDB/caching"
	geminidb "github.com/snissn/gomap-gemini/TreeDB/db"
	"github.com/spf13/cast"
)

const (
	GeminiBackend       BackendType = "gemini"
	GeminiCachedBackend BackendType = "geminicached"
)

func init() {
	registerDBCreator(GeminiBackend, func(name, dir string, opts Options) (DB, error) {
		return NewGeminiDB(name, dir, opts)
	}, false)

	registerDBCreator(GeminiCachedBackend, func(name, dir string, opts Options) (DB, error) {
		return NewGeminiCachedDB(name, dir, opts)
	}, false)
}

// GeminiWrapper wraps geminidb.DB to satisfy cosmosdb.DB interface.
type GeminiWrapper struct {
	*geminidb.DB
}

// NewGeminiDB creates a new Gemini database.
func NewGeminiDB(name, dir string, opts Options) (DB, error) {
	dbPath := filepath.Join(dir, name+".db")

	chunkSize := int64(64 * 1024 * 1024)
	keepRecent := uint64(10000)

	if opts != nil {
		if v := opts.Get("keep_recent"); v != nil {
			keepRecent = cast.ToUint64(v)
		}
	}

	gOpts := geminidb.Options{
		Dir:        dbPath,
		ChunkSize:  chunkSize,
		KeepRecent: keepRecent,
	}

	db, err := geminidb.Open(gOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open gemini db: %w", err)
	}

	return &GeminiWrapper{db}, nil
}

func (db *GeminiWrapper) Iterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachingIteratorWrapper{it, start, end}, nil
}

func (db *GeminiWrapper) ReverseIterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachingIteratorWrapper{it, start, end}, nil
}

// NewBatch returns a new batch.
func (db *GeminiWrapper) NewBatch() Batch {
	return db.DB.NewBatch()
}

// NewBatchWithSize returns a new batch with a size hint.
func (db *GeminiWrapper) NewBatchWithSize(size int) Batch {
	return db.DB.NewBatchWithSize(size)
}

func (db *GeminiWrapper) Print() error {
	return db.DB.Print()
}

// GeminiCachedWrapper wraps geminicaching.DB.
type GeminiCachedWrapper struct {
	*geminicaching.DB
}

// NewGeminiCachedDB creates a new Gemini database with caching.
func NewGeminiCachedDB(name, dir string, opts Options) (DB, error) {
	dbPath := filepath.Join(dir, name+".db")

	chunkSize := int64(64 * 1024 * 1024)
	flushThreshold := int64(4 * 1024 * 1024)
	keepRecent := uint64(10000)

	if opts != nil {
		if v := opts.Get("keep_recent"); v != nil {
			keepRecent = cast.ToUint64(v)
		}
	}

	gOpts := geminidb.Options{
		Dir:        dbPath,
		ChunkSize:  chunkSize,
		KeepRecent: keepRecent,
	}

	// Open underlying backend
	backendDB, err := geminidb.Open(gOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open gemini backend: %w", err)
	}

	// Open caching layer
	db, err := geminicaching.Open(dbPath, backendDB, flushThreshold)
	if err != nil {
		backendDB.Close()
		return nil, fmt.Errorf("failed to open gemini caching: %w", err)
	}

	return &GeminiCachedWrapper{db}, nil
}

func (db *GeminiCachedWrapper) Iterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	// The caching.Iterator satisfies the interface expected by cachingIteratorWrapper
	// (Next, Valid, Key, Value, Close, Error).
	return &cachingIteratorWrapper{it, start, end}, nil
}

func (db *GeminiCachedWrapper) ReverseIterator(start, end []byte) (Iterator, error) {
	it, err := db.DB.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &cachingIteratorWrapper{it, start, end}, nil
}

func (db *GeminiCachedWrapper) NewBatch() Batch {
	return db.DB.NewBatch()
}

func (db *GeminiCachedWrapper) NewBatchWithSize(size int) Batch {
	return db.DB.NewBatchWithSize(size)
}

func (db *GeminiCachedWrapper) Print() error {
	return db.DB.Print()
}
