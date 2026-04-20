package db

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func iavlNodeKey(version int64, nonce uint32) []byte {
	key := make([]byte, 13)
	key[0] = 's'
	binary.BigEndian.PutUint64(key[1:9], uint64(version))
	binary.BigEndian.PutUint32(key[9:13], nonce)
	return key
}

func iavlVersionScanBounds() (start []byte, end []byte) {
	start = make([]byte, 9)
	start[0] = 's'
	binary.BigEndian.PutUint64(start[1:9], uint64(1))
	end = make([]byte, 9)
	end[0] = 's'
	binary.BigEndian.PutUint64(end[1:9], uint64(math.MaxInt64))
	return start, end
}

func iavlVersionFromIteratorKey(t *testing.T, key []byte) int64 {
	t.Helper()
	require.GreaterOrEqual(t, len(key), 9)
	require.Equal(t, byte('s'), key[0])
	return int64(binary.BigEndian.Uint64(key[1:9]))
}

func TestTreeDBReverseIterator_IAVLRange_NoCheckpoint(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, TreeDBBackend, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		cleanupDBDir(dir, name)
	})

	// Mirror IAVL usage where each store is namespaced through PrefixDB.
	store := NewPrefixDB(db, []byte("s/k:minfee/"))

	const versions = 1024
	for v := 1; v <= versions; v++ {
		b := store.NewBatch()
		require.NoError(t, b.Set(iavlNodeKey(int64(v), 1), []byte{1}))
		require.NoError(t, b.Write())
		require.NoError(t, b.Close())
	}

	start, end := iavlVersionScanBounds()
	rit, err := store.ReverseIterator(start, end)
	require.NoError(t, err)
	defer rit.Close()

	require.True(t, rit.Valid(), "reverse iterator should observe written versions without checkpoint")
	got := iavlVersionFromIteratorKey(t, rit.Key())
	require.Equal(t, int64(versions), got)
	require.NoError(t, rit.Error())
}

func TestTreeDBReverseIterator_IAVLRange_WithOtherPrefixes(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, TreeDBBackend, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		cleanupDBDir(dir, name)
	})

	// Add unrelated prefixed stores to ensure range bounds are enforced.
	other := NewPrefixDB(db, []byte("s/k:acc/"))
	target := NewPrefixDB(db, []byte("s/k:minfee/"))

	for v := 1; v <= 64; v++ {
		b := other.NewBatch()
		require.NoError(t, b.Set(iavlNodeKey(int64(v), 1), []byte{1}))
		require.NoError(t, b.Write())
		require.NoError(t, b.Close())
	}

	const versions = 1024
	for v := 1; v <= versions; v++ {
		b := target.NewBatch()
		require.NoError(t, b.Set(iavlNodeKey(int64(v), 1), []byte{1}))
		require.NoError(t, b.Write())
		require.NoError(t, b.Close())
	}

	start, end := iavlVersionScanBounds()
	rit, err := target.ReverseIterator(start, end)
	require.NoError(t, err)
	defer rit.Close()

	require.True(t, rit.Valid(), "reverse iterator should observe target-prefix versions")
	got := iavlVersionFromIteratorKey(t, rit.Key())
	require.Equal(t, int64(versions), got)
	require.NoError(t, rit.Error())
}
