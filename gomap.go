package db

import (
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap"
	gbtree "github.com/snissn/gomap/btree"
)

// GomapBackend represents the gomap B+Tree-backed database.
const GomapBackend BackendType = "gomap"

func init() {
	registerDBCreator(GomapBackend, func(name, dir string, opts Options) (DB, error) {
		return NewGomapDB(name, dir)
	}, false)
}

// GomapDB implements DB on top of gomap + the btree layer for ordering.
type GomapDB struct {
	store *gomap.HashmapDistributed
	tree  *gbtree.Tree
}

var _ DB = (*GomapDB)(nil)

// NewGomapDB creates a new gomap-backed database at dir/name.db.
func NewGomapDB(name, dir string) (*GomapDB, error) {
	dbPath := filepath.Join(dir, name+DBFileSuffix)

	store := &gomap.HashmapDistributed{}
	if err := store.New(dbPath); err != nil {
		return nil, fmt.Errorf("init gomap: %w", err)
	}

	tree, err := gbtree.NewTreeOnGomap(store, "default")
	if err != nil {
		return nil, fmt.Errorf("init btree: %w", err)
	}

	return &GomapDB{
		store: store,
		tree:  tree,
	}, nil
}

// Get implements DB.
func (db *GomapDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	return db.tree.Get(key)
}

// Has implements DB.
func (db *GomapDB) Has(key []byte) (bool, error) {
	val, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

// Set implements DB.
func (db *GomapDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	return db.tree.Put(key, value)
}

// SetSync implements DB.
func (db *GomapDB) SetSync(key, value []byte) error {
	return db.Set(key, value)
}

// Delete implements DB.
func (db *GomapDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return db.tree.Delete(key)
}

// DeleteSync implements DB.
func (db *GomapDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// Close implements DB. gomap has no open file handles to release, so this is a no-op.
func (db *GomapDB) Close() error {
	return nil
}

// Print implements DB.
func (db *GomapDB) Print() error {
	return nil
}

// Stats implements DB.
func (db *GomapDB) Stats() map[string]string {
	stats := db.store.Stats()
	return map[string]string{
		"key_count": fmt.Sprintf("%d", stats.KeyCount),
		"capacity":  fmt.Sprintf("%d", stats.Capacity),
		"data_size": fmt.Sprintf("%d", stats.DataSize),
		"segments":  fmt.Sprintf("%d", stats.Segments),
	}
}

// NewBatch implements DB.
func (db *GomapDB) NewBatch() Batch {
	return newGomapBatch(db)
}

// NewBatchWithSize implements DB.
func (db *GomapDB) NewBatchWithSize(size int) Batch {
	// size is currently informational; we still preallocate slice capacity.
	b := newGomapBatch(db)
	if size > 0 {
		b.ops = make([]gomapOperation, 0, size)
	}
	return b
}

// Iterator implements DB.
func (db *GomapDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	it, err := db.tree.Range(start, end)
	if err != nil {
		return nil, err
	}
	return &gomapIterator{
		start: start,
		end:   end,
		iter:  it,
	}, nil
}

// ReverseIterator implements DB.
func (db *GomapDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	it, err := db.tree.ReverseRange(start, end)
	if err != nil {
		return nil, err
	}
	return &gomapIterator{
		start:   start,
		end:     end,
		revIter: it,
	}, nil
}
