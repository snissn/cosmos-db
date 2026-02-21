package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/tree"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

func init() {
	dbCreator := func(name, dir string, opts Options) (DB, error) {
		return NewTreeDB(name, dir, opts)
	}
	registerDBCreator(TreeDBBackend, dbCreator, false)
}

// TreeDB is a TreeDB backend.
type TreeDB struct {
	db         *treedb.DB
	kv         *treedbadapter.DB
	snap       *treedb.Snapshot
	reuseReads bool
	readBuf    []byte
}

var _ DB = (*TreeDB)(nil)

func (d *TreeDB) PinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
	}
	d.snap = d.db.AcquireSnapshot()
}

func (d *TreeDB) UnpinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
		d.snap = nil
	}
}

func NewTreeDB(name, dir string, opts Options) (*TreeDB, error) {
	_ = opts
	return NewTreeDBAdapter(dir, name)
}

func NewTreeDBAdapter(dir string, name string) (*TreeDB, error) {
	dbPath := filepath.Join(dir, name+DBFileSuffix)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("error creating treedb directory: %w", err)
	}

	tdb, err := treedb.Open(treedb.OptionsFor(treedb.ProfileFast, dbPath))
	if err != nil {
		return nil, err
	}

	adapter := &TreeDB{
		db:         tdb,
		kv:         treedbadapter.Wrap(tdb),
		reuseReads: false,
	}
	return adapter, nil
}

// Get implements DB.
func (d *TreeDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	if d.snap != nil {
		val, err := d.snap.GetUnsafe(key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return val, nil
	}
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	if d.reuseReads {
		val, err := d.db.GetAppend(key, d.readBuf[:0])
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		d.readBuf = val[:0]
		return val, nil
	}
	return d.kv.GetUnsafe(key)
}

// Has implements DB.
func (d *TreeDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	if d.snap != nil {
		return d.snap.Has(key)
	}
	if d.kv == nil {
		return false, treedb.ErrClosed
	}
	return d.kv.Has(key)
}

// Set implements DB.
func (d *TreeDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Set(key, value)
}

// SetSync implements DB.
func (d *TreeDB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.SetSync(key, value)
}

// Delete implements DB.
func (d *TreeDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Delete(key)
}

// DeleteSync implements DB.
func (d *TreeDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.DeleteSync(key)
}

// Iterator implements DB.
func (d *TreeDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

// ReverseIterator implements DB.
func (d *TreeDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

// Close implements DB.
func (d *TreeDB) Close() error {
	if d.db == nil {
		return nil
	}
	d.UnpinSnapshot()
	err := d.db.Close()
	d.db = nil
	d.kv = nil
	return err
}

// NewBatch implements DB.
func (d *TreeDB) NewBatch() Batch {
	return d.NewBatchWithSize(16)
}

// NewBatchWithSize implements DB.
func (d *TreeDB) NewBatchWithSize(size int) Batch {
	if size <= 0 {
		size = 16
	}
	b := &coreBatch{db: d}
	if d.kv != nil {
		kb, err := d.kv.NewBatch()
		if err == nil {
			b.kb = kb
			if sv, ok := kb.(interface{ SetView(key, value []byte) error }); ok {
				b.setView = sv.SetView
			}
			if dv, ok := kb.(interface{ DeleteView(key []byte) error }); ok {
				b.deleteView = dv.DeleteView
			}
		}
	}
	return b
}

// Print implements DB.
func (d *TreeDB) Print() error {
	itr, err := d.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Checkpoint triggers a durable checkpoint in cached mode.
func (d *TreeDB) Checkpoint() error {
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Checkpoint()
}

// Stats implements DB.
func (d *TreeDB) Stats() map[string]string {
	if d.kv == nil {
		return nil
	}
	return d.kv.Stats()
}

// FragmentationReport reports tree fragmentation metrics.
func (d *TreeDB) FragmentationReport() (map[string]string, error) {
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	return d.db.FragmentationReport()
}
