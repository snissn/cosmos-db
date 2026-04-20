package db

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbkv "github.com/snissn/gomap/TreeDB/integration/kvstoreadapter"
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
	db                     *treedb.DB
	kv                     *treedbadapter.DB
	snap                   treedb.Snapshot
	reuseReads             bool
	readBuf                []byte
	forceCheckpointOnWrite bool
	batchWriteMu           sync.Mutex
}

var _ DB = (*TreeDB)(nil)

const envTreeDBForceCheckpointOnWrite = "TREEDB_FORCE_CHECKPOINT_ON_WRITE"
const envTreeDBOpenProfile = treedbkv.EnvOpenProfile
const envTreeDBKeepRecent = treedbkv.EnvKeepRecent
const envTreeDBMemtableMode = treedbkv.EnvMemtableMode

func (d *TreeDB) PinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
	}
	// In cached mode, AcquireSnapshot is backend-only. If we are prioritizing
	// correctness (e.g. IAVL restore), force a backend visibility barrier first
	// so snapshot reads cannot miss recently-written queued/memtable state.
	if d.forceCheckpointOnWrite && d.kv != nil {
		_ = d.kv.Checkpoint()
	}
	d.snap = d.db.AcquireSnapshot()
}

func (d *TreeDB) UnpinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
		d.snap = nil
	}
}

func forceCheckpointOnWriteFromEnv() bool {
	raw, ok := os.LookupEnv(envTreeDBForceCheckpointOnWrite)
	if !ok {
		return false
	}
	on, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return on
}

func (d *TreeDB) maybeCheckpointAfterWrite() error {
	if d == nil || !d.forceCheckpointOnWrite || d.kv == nil {
		return nil
	}
	return d.kv.Checkpoint()
}

func (d *TreeDB) writeSyncBarrier() error {
	if d == nil || d.db == nil {
		return treedb.ErrClosed
	}
	if treedbVisibilityOn() && d.kv != nil {
		stats := d.kv.Stats()
		treedbVisibilityf(
			"barrier pre checkpoint queue_len=%s queue_backlog=%s mutable_bytes=%s",
			stats["treedb.cache.queue_len"],
			stats["treedb.cache.queue_backlog_bytes"],
			stats["treedb.cache.mutable_bytes"],
		)
	}
	// IAVL restore/load paths depend on versioned root-key visibility immediately
	// after Batch.WriteSync boundaries. Keep a strict backend visibility barrier
	// here; lightweight flush has shown missing-version failures under restore.
	if d.kv != nil {
		err := d.kv.Checkpoint()
		if treedbVisibilityOn() {
			stats := d.kv.Stats()
			treedbVisibilityf(
				"barrier post checkpoint err=%v queue_len=%s queue_backlog=%s mutable_bytes=%s",
				err,
				stats["treedb.cache.queue_len"],
				stats["treedb.cache.queue_backlog_bytes"],
				stats["treedb.cache.mutable_bytes"],
			)
		}
		return err
	}
	return d.db.Checkpoint()
}

func (d *TreeDB) withSerializedBatchWrite(fn func() error) error {
	if d == nil || d.kv == nil {
		return treedb.ErrClosed
	}
	d.batchWriteMu.Lock()
	defer d.batchWriteMu.Unlock()
	return fn()
}

func NewTreeDB(name, dir string, opts Options) (*TreeDB, error) {
	_ = opts
	return NewTreeDBAdapter(dir, name)
}

func NewTreeDBAdapter(dir string, name string) (*TreeDB, error) {
	opened, err := treedbkv.Open(treedbkv.OpenConfig{
		ParentDir:                   dir,
		Name:                        name,
		DBFileSuffix:                DBFileSuffix,
		AdapterName:                 "TreeDB",
		DefaultProfile:              treedb.ProfileWALOnFast,
		DefaultKeepRecent:           1,
		DefaultAdaptiveMemtableBase: "hash_sorted",
		ProfileEnvKey:               envTreeDBOpenProfile,
		KeepRecentEnvKey:            envTreeDBKeepRecent,
		MemtableModeEnvKey:          envTreeDBMemtableMode,
	})
	if err != nil {
		return nil, err
	}

	adapter := &TreeDB{
		db:                     opened.DB,
		kv:                     opened.KV,
		reuseReads:             false,
		forceCheckpointOnWrite: forceCheckpointOnWriteFromEnv(),
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
		if treedbVisibilityTrackKey(key) {
			treedbVisibilityf("get source=snapshot key=%x val_nil=%t val_len=%d err=%v", key, val == nil, len(val), err)
		}
		if version, prefix, ok := prefixedIAVLRootVersion(key); ok {
			treedbVisibilityf("get source=snapshot prefix=%q key=%x version=%d val_nil=%t val_len=%d err=%v", prefix, key, version, val == nil, len(val), err)
		}
		if isRootMultiMetaKey(key) {
			treedbVisibilityf("get-meta source=snapshot key=%q val_nil=%t val_len=%d err=%v", key, val == nil, len(val), err)
		}
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
		if treedbVisibilityTrackKey(key) {
			treedbVisibilityf("get source=getappend key=%x val_nil=%t val_len=%d err=%v", key, val == nil, len(val), err)
		}
		if version, prefix, ok := prefixedIAVLRootVersion(key); ok {
			treedbVisibilityf("get source=getappend prefix=%q key=%x version=%d val_nil=%t val_len=%d err=%v", prefix, key, version, val == nil, len(val), err)
		}
		if isRootMultiMetaKey(key) {
			treedbVisibilityf("get-meta source=getappend key=%q val_nil=%t val_len=%d err=%v", key, val == nil, len(val), err)
		}
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		d.readBuf = val[:0]
		return val, nil
	}
	val, err := d.kv.GetUnsafe(key)
	if treedbVisibilityTrackKey(key) {
		treedbVisibilityf("get source=kv key=%x val_nil=%t val_len=%d err=%v", key, val == nil, len(val), err)
	}
	if version, prefix, ok := prefixedIAVLRootVersion(key); ok {
		treedbVisibilityf("get source=kv prefix=%q key=%x version=%d val_nil=%t val_len=%d err=%v", prefix, key, version, val == nil, len(val), err)
	}
	if isRootMultiMetaKey(key) {
		treedbVisibilityf("get-meta source=kv key=%q val_nil=%t val_len=%d err=%v", key, val == nil, len(val), err)
	}
	return val, err
}

// GetAppend fetches the value of the given key into dst when supported.
// Missing keys return (nil, nil) to match DB.Get semantics.
func (d *TreeDB) GetAppend(key, dst []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	if d.snap != nil {
		val, err := d.snap.GetAppend(key, dst)
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
	val, err := d.db.GetAppend(key, dst)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

// Has implements DB.
func (d *TreeDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	if d.snap != nil {
		ok, err := d.snap.Has(key)
		if treedbVisibilityTrackKey(key) {
			treedbVisibilityf("has source=snapshot key=%x ok=%t err=%v", key, ok, err)
		}
		if version, prefix, match := prefixedIAVLRootVersion(key); match {
			treedbVisibilityf("has source=snapshot prefix=%q key=%x version=%d ok=%t err=%v", prefix, key, version, ok, err)
		}
		if isRootMultiMetaKey(key) {
			treedbVisibilityf("has-meta source=snapshot key=%q ok=%t err=%v", key, ok, err)
		}
		return ok, err
	}
	if d.kv == nil {
		return false, treedb.ErrClosed
	}
	ok, err := d.kv.Has(key)
	if treedbVisibilityTrackKey(key) {
		treedbVisibilityf("has source=kv key=%x ok=%t err=%v", key, ok, err)
	}
	if version, prefix, match := prefixedIAVLRootVersion(key); match {
		treedbVisibilityf("has source=kv prefix=%q key=%x version=%d ok=%t err=%v", prefix, key, version, ok, err)
	}
	if isRootMultiMetaKey(key) {
		treedbVisibilityf("has-meta source=kv key=%q ok=%t err=%v", key, ok, err)
	}
	return ok, err
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
	if err := d.kv.Set(key, value); err != nil {
		return err
	}
	if version, prefix, ok := prefixedIAVLRootVersion(key); ok {
		treedbVisibilityf("set prefix=%q key=%x version=%d val_len=%d", prefix, key, version, len(value))
	}
	return d.maybeCheckpointAfterWrite()
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
	if err := d.kv.SetSync(key, value); err != nil {
		return err
	}
	if version, prefix, ok := prefixedIAVLRootVersion(key); ok {
		treedbVisibilityf("setsync prefix=%q key=%x version=%d val_len=%d", prefix, key, version, len(value))
	}
	return d.maybeCheckpointAfterWrite()
}

// Delete implements DB.
func (d *TreeDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	if err := d.kv.Delete(key); err != nil {
		return err
	}
	if version, prefix, ok := prefixedIAVLRootVersion(key); ok {
		treedbVisibilityf("delete prefix=%q key=%x version=%d", prefix, key, version)
	}
	return d.maybeCheckpointAfterWrite()
}

// DeleteSync implements DB.
func (d *TreeDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	if err := d.kv.DeleteSync(key); err != nil {
		return err
	}
	if version, prefix, ok := prefixedIAVLRootVersion(key); ok {
		treedbVisibilityf("deletesync prefix=%q key=%x version=%d", prefix, key, version)
	}
	return d.maybeCheckpointAfterWrite()
}

// Iterator implements DB.
func (d *TreeDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.forwardIteratorWithIAVLFallback(start, end)
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
