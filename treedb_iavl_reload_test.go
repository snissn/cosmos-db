package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func iavlStoreNodeKey(version uint64, nonce uint32) []byte {
	key := make([]byte, 13)
	key[0] = 's'
	binary.BigEndian.PutUint64(key[1:9], version)
	binary.BigEndian.PutUint32(key[9:13], nonce)
	return key
}

func iavlStoreVersionScanBounds() ([]byte, []byte) {
	start := make([]byte, 9)
	start[0] = 's'
	binary.BigEndian.PutUint64(start[1:9], uint64(1))
	end := make([]byte, 9)
	end[0] = 's'
	binary.BigEndian.PutUint64(end[1:9], uint64(math.MaxInt64))
	return start, end
}

func iavlVersionFromKey(t *testing.T, key []byte) uint64 {
	t.Helper()
	require.GreaterOrEqual(t, len(key), 9)
	require.Equal(t, byte('s'), key[0])
	return binary.BigEndian.Uint64(key[1:9])
}

// Reproduces the "fresh load cannot discover latest version" class of failures
// by writing many independent prefixed stores at a high version and then
// reopening and scanning via ReverseIterator (the same operation used by IAVL
// latest-version discovery).
func TestTreeDBIAVLReloadAcrossPrefixes(t *testing.T) {
	const rounds = 3
	const targetVersion = uint64(9_992_000)
	const keysPerStore = 1024

	stores := []string{
		"acc", "authz", "bank", "distribution", "feegrant",
		"gov", "ibc", "icahost", "minfee", "signal", "staking",
		"transfer", "upgrade", "warp",
	}

	for round := 0; round < rounds; round++ {
		name := fmt.Sprintf("test_%x_%d", randStr(12), round)
		dir := os.TempDir()

		db, err := NewDB(name, TreeDBBackend, dir)
		require.NoError(t, err)

		rootValues := make(map[string][]byte, len(stores))
		for si, storeName := range stores {
			prefix := []byte("s/k:" + storeName + "/")
			pdb := NewPrefixDB(db, prefix)
			b := pdb.NewBatch()
			for i := 0; i < keysPerStore; i++ {
				nonce := uint32((i % 4095) + 2)
				key := iavlStoreNodeKey(targetVersion, nonce)
				val := bytes.Repeat([]byte{byte((si % 251) + 1)}, 96)
				require.NoError(t, b.Set(key, val), "round=%d store=%s nonce=%d", round, storeName, nonce)
			}
			rootKey := iavlStoreNodeKey(targetVersion, 1)
			rootVal := bytes.Repeat([]byte{byte((si % 251) + 1)}, 73)
			rootValues[storeName] = rootVal
			require.NoError(t, b.Set(rootKey, rootVal), "round=%d store=%s root set", round, storeName)
			require.NoError(t, b.WriteSync(), "round=%d store=%s writesync", round, storeName)
			require.NoError(t, b.Close(), "round=%d store=%s close", round, storeName)
		}
		require.NoError(t, db.Close())

		db, err = NewDB(name, TreeDBBackend, dir)
		require.NoError(t, err)

		for _, storeName := range stores {
			prefix := []byte("s/k:" + storeName + "/")
			pdb := NewPrefixDB(db, prefix)

			rootKey := iavlStoreNodeKey(targetVersion, 1)
			ok, err := pdb.Has(rootKey)
			require.NoError(t, err, "round=%d store=%s has root", round, storeName)
			require.True(t, ok, "round=%d store=%s root missing", round, storeName)

			got, err := pdb.Get(rootKey)
			require.NoError(t, err, "round=%d store=%s get root", round, storeName)
			require.Equal(t, rootValues[storeName], got, "round=%d store=%s root mismatch", round, storeName)

			start, end := iavlStoreVersionScanBounds()
			rit, err := pdb.ReverseIterator(start, end)
			require.NoError(t, err, "round=%d store=%s reverse iterator", round, storeName)
			require.True(t, rit.Valid(), "round=%d store=%s reverse iterator invalid", round, storeName)
			require.Equal(t, targetVersion, iavlVersionFromKey(t, rit.Key()), "round=%d store=%s latest version", round, storeName)
			require.NoError(t, rit.Error(), "round=%d store=%s reverse iterator err", round, storeName)
			require.NoError(t, rit.Close())
		}

		require.NoError(t, db.Close())
		cleanupDBDir(dir, name)
	}
}
