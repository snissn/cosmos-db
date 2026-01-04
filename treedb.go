package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/tree"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

const memtableMode = "adaptive"

const (
	envDisableWAL          = "TREEDB_BENCH_DISABLE_WAL"
	envDisableBG           = "TREEDB_BENCH_DISABLE_BG"
	envRelaxedSync         = "TREEDB_BENCH_RELAXED_SYNC"
	envDisableValueLog     = "TREEDB_BENCH_DISABLE_VALUE_LOG"
	envDisableReadChecksum = "TREEDB_BENCH_DISABLE_READ_CHECKSUM"
	envAllowUnsafe         = "TREEDB_BENCH_ALLOW_UNSAFE"
	envMode                = "TREEDB_BENCH_MODE"
	envPinSnapshot         = "TREEDB_BENCH_PIN_SNAPSHOT"
	envReuseReads          = "TREEDB_BENCH_REUSE_READS"
	envForceValuePointers  = "TREEDB_FORCE_VALUE_POINTERS"
	envValueLogThreshold   = "TREEDB_VALUELOG_POINTER_THRESHOLD"
	envSlabCompression     = "TREEDB_SLAB_COMPRESSION"
	envSlabCompMinBytes    = "TREEDB_SLAB_COMPRESSION_MIN_BYTES"
	envSlabCompMinSavings  = "TREEDB_SLAB_COMPRESSION_MIN_SAVINGS"
	envDisableIndexVacuum  = "TREEDB_DISABLE_INDEX_VACUUM"
	envSyncMode            = "TREEDB_SYNC_MODE"
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

func envBool(name string, defaultValue bool) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n != 0
	}
	return defaultValue
}

func envInt64(name string, defaultValue int64) int64 {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return defaultValue
	}
	return n
}

func envInt(name string, defaultValue int) int {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultValue
	}
	return n
}

func envString(name string, defaultValue string) string {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultValue
	}
	return v
}

func parseSlabCompression(name string, minBytes int, minSavings int) slab.CompressionOptions {
	opts := slab.CompressionOptions{
		Kind:            slab.CompressionNone,
		MinBytes:        minBytes,
		MinSavingsBytes: minSavings,
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "none", "off", "false":
		opts.Kind = slab.CompressionNone
	case "zstd":
		opts.Kind = slab.CompressionZSTD
	}
	return opts
}

func setAllowUnsafe(opts *treedb.Options, allow bool) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName("AllowUnsafe")
	if !field.IsValid() || !field.CanSet() {
		return
	}
	if field.Kind() == reflect.Bool {
		field.SetBool(allow)
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

	disableWAL := envBool(envDisableWAL, false)
	disableBG := envBool(envDisableBG, false)
	pinSnapshot := envBool(envPinSnapshot, false)
	reuseReads := envBool(envReuseReads, false)
	relaxedSync := envBool(envRelaxedSync, true)
	disableValueLog := envBool(envDisableValueLog, false)
	disableReadChecksum := envBool(envDisableReadChecksum, true)
	forceValuePointers := envBool(envForceValuePointers, true)
	valueLogThreshold := envInt(envValueLogThreshold, 256)
	slabCompression := parseSlabCompression(
		envString(envSlabCompression, "zstd"),
		envInt(envSlabCompMinBytes, 0),
		envInt(envSlabCompMinSavings, 0),
	)
	disableIndexVacuum := envBool(envDisableIndexVacuum, false)
	syncMode := envBool(envSyncMode, false)
	_, allowUnsafeSet := os.LookupEnv(envAllowUnsafe)
	allowUnsafe := envBool(envAllowUnsafe, false)
	if !allowUnsafeSet && (disableWAL || relaxedSync || disableReadChecksum) {
		allowUnsafe = true
	}

	mode := treedb.ModeCached
	switch strings.ToLower(envString(envMode, "cached")) {
	case "backend", "raw", "uncached":
		mode = treedb.ModeBackend
	}

	openOpts := treedb.Options{
		Dir:          dbPath,
		Mode:         mode,
		MemtableMode: memtableMode,

		// --- "Unsafe" Performance Options ---
		DisableWAL:          disableWAL,
		DisableValueLog:     disableValueLog,
		RelaxedSync:         relaxedSync,
		DisableReadChecksum: disableReadChecksum,

		// --- Tuning for High-Throughput & Large Values ---
		FlushThreshold:        64 * 1024 * 1024,
		FlushBuildConcurrency: 4,
		ChunkSize:             64 * 1024 * 1024,

		ForceValuePointers:       forceValuePointers,
		ValueLogPointerThreshold: valueLogThreshold,
		SlabCompression:          slabCompression,

		PreferAppendAlloc:             false,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: 15 * time.Second,

		// Add Value Log Compaction
		//BackgroundCompactionInterval:  1 * time.Second,
		//BackgroundCompactionDeadRatio: 0.1,
	}
	setAllowUnsafe(&openOpts, allowUnsafe)

	if disableBG {
		// Background tasks can dominate profile lock/wait time and obscure the
		// hot path; disable them for tighter profiling loops.
		openOpts.BackgroundIndexVacuumInterval = -1
		openOpts.BackgroundCheckpointInterval = -1
		openOpts.MaxWALBytes = -1
		openOpts.BackgroundCheckpointIdleDuration = -1
	}
	if disableIndexVacuum || syncMode {
		openOpts.BackgroundIndexVacuumInterval = -1
	}

	tdb, err := treedb.Open(openOpts)
	if err != nil {
		return nil, err
	}

	adapter := &TreeDB{
		db:         tdb,
		kv:         treedbadapter.Wrap(tdb),
		reuseReads: reuseReads,
	}
	if pinSnapshot {
		adapter.PinSnapshot()
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
