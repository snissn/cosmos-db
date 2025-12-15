//go:build rocksdb
// +build rocksdb

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

func TestRocksDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	_, ok := db.(*RocksDB)
	require.True(t, ok)
}

func TestWithRocksDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rocksdb")

	db, err := NewRocksDB(path, "", nil)
	require.NoError(t, err)

	t.Run("RocksDB", func(t *testing.T) { Run(t, db) })
}

func TestRocksDBStats(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	require.NotEmpty(t, db.Stats())
}

func TestRocksDBWithOptions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rocksdb")

	opts := make(OptionsMap, 0)
	opts["maxopenfiles"] = 1000

	defaultOpts := defaultRocksdbOptions()
	files := cast.ToInt(opts.Get("maxopenfiles"))
	defaultOpts.SetMaxOpenFiles(files)
	require.Equal(t, opts["maxopenfiles"], defaultOpts.GetMaxOpenFiles())

	db, err := NewRocksDB(path, "", opts)
	require.NoError(t, err)

	t.Run("RocksDB", func(t *testing.T) { Run(t, db) })
}

func BenchmarkRocksDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		require.NoError(b, db.Close())
		cleanupDBDir(dir, name)
	}()

	benchmarkRandomReadsWrites(b, db)
}

func BenchmarkRocksDBRangeScans1M(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		require.NoError(b, db.Close())
		cleanupDBDir(dir, name)
	}()

	benchmarkRangeScans(b, db, int64(1e6))
}

func BenchmarkRocksDBRangeScans10M(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		require.NoError(b, db.Close())
		cleanupDBDir(dir, name)
	}()

	benchmarkRangeScans(b, db, int64(10e6))
}
