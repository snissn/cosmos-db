package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap/TreeDB"
	"github.com/spf13/cast"
)

func init() {
	registerDBCreator(TreeDBBackend, NewTreeDBDB, false)
}

type TreeDB struct {
	db *treedb.DB

	closeOnce sync.Once
	closeErr  error
}

var _ DB = (*TreeDB)(nil)

func NewTreeDBDB(name, dir string, opts Options) (DB, error) {
	dbDir := filepath.Join(dir, name+DBFileSuffix)
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, err
	}

	openOpts := treedb.Options{
		Dir:  dbDir,
		Mode: treedb.ModeCached,
	}

	if opts != nil {
		if v := opts.Get("treedb.mode"); v != nil {
			mode := cast.ToString(v)
			switch mode {
			case "", "cached":
				openOpts.Mode = treedb.ModeCached
			case "backend":
				openOpts.Mode = treedb.ModeBackend
			default:
				return nil, fmt.Errorf("invalid treedb.mode %q (expected \"cached\" or \"backend\")", mode)
			}
		}

		if v := opts.Get("treedb.flush_threshold_bytes"); v != nil {
			openOpts.FlushThreshold = cast.ToInt64(v)
		}
		if v := opts.Get("treedb.chunk_size_bytes"); v != nil {
			openOpts.ChunkSize = cast.ToInt64(v)
		}
		if v := opts.Get("treedb.keep_recent"); v != nil {
			openOpts.KeepRecent = cast.ToUint64(v)
		}
	}

	tdb, err := treedb.Open(openOpts)
	if err != nil {
		return nil, err
	}

	return &TreeDB{db: tdb}, nil
}

func (db *TreeDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	value, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return cp(value), nil
}

func (db *TreeDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	return db.db.Has(key)
}

func (db *TreeDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	return db.db.Set(key, value)
}

func (db *TreeDB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	return db.db.SetSync(key, value)
}

func (db *TreeDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return db.db.Delete(key)
}

func (db *TreeDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return db.db.DeleteSync(key)
}

func (db *TreeDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr, err := db.db.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return newTreeDBIterator(itr, start, end), nil
}

func (db *TreeDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr, err := db.db.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return newTreeDBIterator(itr, start, end), nil
}

func (db *TreeDB) Close() error {
	db.closeOnce.Do(func() {
		if db.db != nil {
			db.closeErr = db.db.Close()
			db.db = nil
		}
	})
	return db.closeErr
}

func (db *TreeDB) NewBatch() Batch {
	return newTreeDBBatch(db, 0)
}

func (db *TreeDB) NewBatchWithSize(size int) Batch {
	return newTreeDBBatch(db, size)
}

func (db *TreeDB) Print() error {
	return db.db.Print()
}

func (db *TreeDB) Stats() map[string]string {
	return db.db.Stats()
}
