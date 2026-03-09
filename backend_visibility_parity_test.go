package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"testing"

	gogotypes "github.com/cosmos/gogoproto/types"
	"github.com/stretchr/testify/require"
)

type backendVisibilityCase struct {
	name    string
	backend BackendType
	profile string
}

func runWithBackendVisibilityEnv(t *testing.T, tc backendVisibilityCase, fn func()) {
	t.Helper()
	if tc.backend == TreeDBBackend {
		t.Setenv(envTreeDBOpenProfile, tc.profile)
		t.Setenv(envTreeDBForceCheckpointOnWrite, "0")
	}
	fn()
}

func parityIAVLStoreNodeKey(version uint64, nonce uint32) []byte {
	key := make([]byte, 13)
	key[0] = 's'
	binary.BigEndian.PutUint64(key[1:9], version)
	binary.BigEndian.PutUint32(key[9:13], nonce)
	return key
}

func parityIAVLStoreVersionScanBounds() ([]byte, []byte) {
	start := make([]byte, 9)
	start[0] = 's'
	binary.BigEndian.PutUint64(start[1:9], uint64(1))
	end := make([]byte, 9)
	end[0] = 's'
	binary.BigEndian.PutUint64(end[1:9], uint64(math.MaxInt64))
	return start, end
}

func parityIAVLVersionFromKey(t *testing.T, key []byte) uint64 {
	t.Helper()
	require.GreaterOrEqual(t, len(key), 9)
	require.Equal(t, byte('s'), key[0])
	return binary.BigEndian.Uint64(key[1:9])
}

func latestVersionViaReverseIterator(t *testing.T, db DB) uint64 {
	t.Helper()
	start, end := parityIAVLStoreVersionScanBounds()
	rit, err := db.ReverseIterator(start, end)
	require.NoError(t, err)
	defer func() { require.NoError(t, rit.Close()) }()
	require.True(t, rit.Valid(), "reverse iterator should find at least one versioned key")
	require.NoError(t, rit.Error())
	return parityIAVLVersionFromKey(t, rit.Key())
}

func TestBackendIAVLReloadAcrossPrefixesParity(t *testing.T) {
	const targetVersion = uint64(9_992_000)
	const keysPerStore = 1024

	stores := []string{
		"acc", "authz", "bank", "distribution", "feegrant",
		"gov", "ibc", "icahost", "minfee", "signal", "staking",
		"transfer", "upgrade", "warp",
	}

	cases := []backendVisibilityCase{
		{name: "goleveldb", backend: GoLevelDBBackend},
		{name: "treedb_fast", backend: TreeDBBackend, profile: "fast"},
		{name: "treedb_wal_on_fast", backend: TreeDBBackend, profile: "wal_on_fast"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWithBackendVisibilityEnv(t, tc, func() {
				name := fmt.Sprintf("test_%x_%s", randStr(12), tc.name)
				dir := os.TempDir()
				db, err := NewDB(name, tc.backend, dir)
				require.NoError(t, err)

				rootValues := make(map[string][]byte, len(stores))
				for si, storeName := range stores {
					prefix := []byte("s/k:" + storeName + "/")
					pdb := NewPrefixDB(db, prefix)
					b := pdb.NewBatch()
					for i := 0; i < keysPerStore; i++ {
						nonce := uint32((i % 4095) + 2)
						key := parityIAVLStoreNodeKey(targetVersion, nonce)
						val := bytes.Repeat([]byte{byte((si % 251) + 1)}, 96)
						require.NoError(t, b.Set(key, val), "store=%s nonce=%d", storeName, nonce)
					}
					rootKey := parityIAVLStoreNodeKey(targetVersion, 1)
					rootVal := bytes.Repeat([]byte{byte((si % 251) + 1)}, 73)
					rootValues[storeName] = rootVal
					require.NoError(t, b.Set(rootKey, rootVal), "store=%s root set", storeName)
					require.NoError(t, b.WriteSync(), "store=%s writesync", storeName)
					require.NoError(t, b.Close(), "store=%s close", storeName)
				}

				for _, storeName := range stores {
					prefix := []byte("s/k:" + storeName + "/")
					pdb := NewPrefixDB(db, prefix)
					rootKey := parityIAVLStoreNodeKey(targetVersion, 1)
					got, err := pdb.Get(rootKey)
					require.NoError(t, err, "same-handle get store=%s", storeName)
					require.Equal(t, rootValues[storeName], got, "same-handle root mismatch store=%s", storeName)
					require.Equal(t, targetVersion, latestVersionViaReverseIterator(t, pdb), "same-handle latest version store=%s", storeName)
				}

				require.NoError(t, db.Close())
				db, err = NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				t.Cleanup(func() {
					if db != nil {
						require.NoError(t, db.Close())
					}
					cleanupDBDir(dir, name)
				})

				for _, storeName := range stores {
					prefix := []byte("s/k:" + storeName + "/")
					pdb := NewPrefixDB(db, prefix)
					rootKey := parityIAVLStoreNodeKey(targetVersion, 1)
					got, err := pdb.Get(rootKey)
					require.NoError(t, err, "reopen get store=%s", storeName)
					require.Equal(t, rootValues[storeName], got, "reopen root mismatch store=%s", storeName)
					require.Equal(t, targetVersion, latestVersionViaReverseIterator(t, pdb), "reopen latest version store=%s", storeName)
				}
			})
		})
	}
}

func TestBackendMetadataVisibilityParityAfterHeavyPrefixedWrites(t *testing.T) {
	stores := []string{
		"acc", "authz", "bank", "blob", "capability",
		"circuit", "consensus", "distribution", "evidence", "feegrant",
		"gov", "hyperlane", "ibc", "icahost", "minfee",
		"mint", "packetfowardmiddleware", "params", "signal", "slashing",
		"staking", "transfer", "upgrade", "warp",
	}
	const height = uint64(9_993_000)
	const keysPerStore = 4096

	cases := []backendVisibilityCase{
		{name: "goleveldb", backend: GoLevelDBBackend},
		{name: "treedb_fast", backend: TreeDBBackend, profile: "fast"},
		{name: "treedb_wal_on_fast", backend: TreeDBBackend, profile: "wal_on_fast"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWithBackendVisibilityEnv(t, tc, func() {
				name := fmt.Sprintf("test_%x_%s", randStr(12), tc.name)
				dir := os.TempDir()
				db, err := NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, db.Close())
					cleanupDBDir(dir, name)
				})

				for si, storeName := range stores {
					pdb := NewPrefixDB(db, []byte("s/k:"+storeName+"/"))
					b := pdb.NewBatch()
					for i := 0; i < keysPerStore; i++ {
						nonce := uint32((i % 4095) + 2)
						key := parityIAVLStoreNodeKey(height, nonce)
						val := bytes.Repeat([]byte{byte((si % 251) + 1)}, 96)
						require.NoError(t, b.Set(key, val), "store=%s nonce=%d", storeName, nonce)
					}
					require.NoError(t, b.Set(parityIAVLStoreNodeKey(height, 1), bytes.Repeat([]byte{0xA5}, 73)), "store=%s root", storeName)
					require.NoError(t, b.WriteSync(), "store=%s writesync", storeName)
					require.NoError(t, b.Close(), "store=%s close", storeName)
				}

				latestValue, err := gogotypes.StdInt64Marshal(int64(height))
				require.NoError(t, err)

				metadataBatch := db.NewBatch()
				commitInfoKey := []byte(fmt.Sprintf("s/%d", height))
				commitInfoValue := bytes.Repeat([]byte{0x7C}, 256)
				require.NoError(t, metadataBatch.Set(commitInfoKey, commitInfoValue))
				require.NoError(t, metadataBatch.Set([]byte("s/latest"), latestValue))
				require.NoError(t, metadataBatch.WriteSync())
				require.NoError(t, metadataBatch.Close())

				gotLatest, err := db.Get([]byte("s/latest"))
				require.NoError(t, err)
				require.Equal(t, latestValue, gotLatest)

				ok, err := db.Has(commitInfoKey)
				require.NoError(t, err)
				require.True(t, ok)

				require.NoError(t, db.Close())
				db = nil
				db, err = NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				gotLatest, err = db.Get([]byte("s/latest"))
				require.NoError(t, err)
				require.Equal(t, latestValue, gotLatest)

				ok, err = db.Has(commitInfoKey)
				require.NoError(t, err)
				require.True(t, ok)
			})
		})
	}
}

func TestBackendStagedVersionDiscoveryParity(t *testing.T) {
	const targetVersion = uint64(10_001_000)
	const intermediateKeys = 10_000
	const totalKeys = 12_500

	cases := []backendVisibilityCase{
		{name: "goleveldb", backend: GoLevelDBBackend},
		{name: "treedb_fast", backend: TreeDBBackend, profile: "fast"},
		{name: "treedb_wal_on_fast", backend: TreeDBBackend, profile: "wal_on_fast"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWithBackendVisibilityEnv(t, tc, func() {
				name := fmt.Sprintf("test_%x_%s", randStr(12), tc.name)
				dir := os.TempDir()
				db, err := NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				t.Cleanup(func() {
					if db != nil {
						require.NoError(t, db.Close())
					}
					cleanupDBDir(dir, name)
				})

				pdb := NewPrefixDB(db, []byte("s/k:staking/"))
				firstBatch := pdb.NewBatch()
				for i := 0; i < intermediateKeys; i++ {
					nonce := uint32(i + 2)
					key := parityIAVLStoreNodeKey(targetVersion, nonce)
					val := bytes.Repeat([]byte{byte((i % 251) + 1)}, 96)
					require.NoError(t, firstBatch.Set(key, val), "first batch nonce=%d", nonce)
				}
				require.NoError(t, firstBatch.Write(), "intermediate write")
				require.NoError(t, firstBatch.Close())

				finalBatch := pdb.NewBatch()
				for i := intermediateKeys; i < totalKeys; i++ {
					nonce := uint32(i + 2)
					key := parityIAVLStoreNodeKey(targetVersion, nonce)
					val := bytes.Repeat([]byte{byte((i % 251) + 1)}, 96)
					require.NoError(t, finalBatch.Set(key, val), "final batch nonce=%d", nonce)
				}
				rootKey := parityIAVLStoreNodeKey(targetVersion, 1)
				rootVal := bytes.Repeat([]byte{0xAB}, 73)
				require.NoError(t, finalBatch.Set(rootKey, rootVal))
				require.NoError(t, finalBatch.WriteSync(), "final writesync")
				require.NoError(t, finalBatch.Close())

				fresh := NewPrefixDB(db, []byte("s/k:staking/"))
				require.Equal(t, targetVersion, latestVersionViaReverseIterator(t, fresh), "same-handle latest version")

				ok, err := fresh.Has(rootKey)
				require.NoError(t, err)
				require.True(t, ok, "same-handle root should exist")

				gotRoot, err := fresh.Get(rootKey)
				require.NoError(t, err)
				require.Equal(t, rootVal, gotRoot, "same-handle root mismatch")

				probeKey := parityIAVLStoreNodeKey(targetVersion, uint32(totalKeys))
				probeVal, err := fresh.Get(probeKey)
				require.NoError(t, err)
				require.NotNil(t, probeVal, "same-handle staged key missing")

				require.NoError(t, db.Close())
				db = nil
				db, err = NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				reopened := NewPrefixDB(db, []byte("s/k:staking/"))
				require.Equal(t, targetVersion, latestVersionViaReverseIterator(t, reopened), "reopen latest version")
				ok, err = reopened.Has(rootKey)
				require.NoError(t, err)
				require.True(t, ok, "reopen root should exist")
				gotRoot, err = reopened.Get(rootKey)
				require.NoError(t, err)
				require.Equal(t, rootVal, gotRoot, "reopen root mismatch")
				probeVal, err = reopened.Get(probeKey)
				require.NoError(t, err)
				require.NotNil(t, probeVal, "reopen staged key missing")
			})
		})
	}
}
