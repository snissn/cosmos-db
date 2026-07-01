package db

import (
	"bytes"
	"fmt"
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
	}
	fn()
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
		{name: "treedb_command_wal_durable", backend: TreeDBBackend, profile: "command_wal_durable"},
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
						key := iavlStoreNodeKey(targetVersion, nonce)
						val := bytes.Repeat([]byte{byte((si % 251) + 1)}, 96)
						require.NoError(t, b.Set(key, val), "store=%s nonce=%d", storeName, nonce)
					}
					rootKey := iavlStoreNodeKey(targetVersion, 1)
					rootVal := bytes.Repeat([]byte{byte((si % 251) + 1)}, 73)
					rootValues[storeName] = rootVal
					require.NoError(t, b.Set(rootKey, rootVal), "store=%s root set", storeName)
					require.NoError(t, b.WriteSync(), "store=%s writesync", storeName)
					require.NoError(t, b.Close(), "store=%s close", storeName)
				}

				// Fresh logical readers on the same open DB handle should observe the
				// latest IAVL-style version keys exactly like a reopened DB does.
				for _, storeName := range stores {
					prefix := []byte("s/k:" + storeName + "/")
					pdb := NewPrefixDB(db, prefix)
					rootKey := iavlStoreNodeKey(targetVersion, 1)
					got, err := pdb.Get(rootKey)
					require.NoError(t, err, "same-handle get store=%s", storeName)
					require.Equal(t, rootValues[storeName], got, "same-handle root mismatch store=%s", storeName)

					start, end := iavlStoreVersionScanBounds()
					rit, err := pdb.ReverseIterator(start, end)
					require.NoError(t, err, "same-handle reverse iterator store=%s", storeName)
					require.True(t, rit.Valid(), "same-handle reverse iterator invalid store=%s", storeName)
					require.Equal(t, targetVersion, iavlVersionFromKey(t, rit.Key()), "same-handle latest version store=%s", storeName)
					require.NoError(t, rit.Error(), "same-handle reverse iterator err store=%s", storeName)
					require.NoError(t, rit.Close())
				}

				require.NoError(t, db.Close())

				db, err = NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, db.Close())
					cleanupDBDir(dir, name)
				})

				for _, storeName := range stores {
					prefix := []byte("s/k:" + storeName + "/")
					pdb := NewPrefixDB(db, prefix)

					rootKey := iavlStoreNodeKey(targetVersion, 1)
					got, err := pdb.Get(rootKey)
					require.NoError(t, err, "reopen get store=%s", storeName)
					require.Equal(t, rootValues[storeName], got, "reopen root mismatch store=%s", storeName)

					start, end := iavlStoreVersionScanBounds()
					rit, err := pdb.ReverseIterator(start, end)
					require.NoError(t, err, "reopen reverse iterator store=%s", storeName)
					require.True(t, rit.Valid(), "reopen reverse iterator invalid store=%s", storeName)
					require.Equal(t, targetVersion, iavlVersionFromKey(t, rit.Key()), "reopen latest version store=%s", storeName)
					require.NoError(t, rit.Error(), "reopen reverse iterator err store=%s", storeName)
					require.NoError(t, rit.Close())
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
		{name: "treedb_command_wal_durable", backend: TreeDBBackend, profile: "command_wal_durable"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWithBackendVisibilityEnv(t, tc, func() {
				name := fmt.Sprintf("test_%x_%s", randStr(12), tc.name)
				dir := os.TempDir()
				db, err := NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				t.Cleanup(func() { cleanupDBDir(dir, name) })

				for si, storeName := range stores {
					pdb := NewPrefixDB(db, []byte("s/k:"+storeName+"/"))
					b := pdb.NewBatch()
					for i := 0; i < keysPerStore; i++ {
						nonce := uint32((i % 4095) + 2)
						key := iavlStoreNodeKey(height, nonce)
						val := bytes.Repeat([]byte{byte((si % 251) + 1)}, 96)
						require.NoError(t, b.Set(key, val), "store=%s nonce=%d", storeName, nonce)
					}
					require.NoError(t, b.Set(iavlStoreNodeKey(height, 1), bytes.Repeat([]byte{0xA5}, 73)), "store=%s root", storeName)
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
				require.Equal(t, latestValue, gotLatest, "s/latest same-handle visibility")

				ok, err := db.Has(commitInfoKey)
				require.NoError(t, err)
				require.True(t, ok, "s/<height> same-handle visibility")

				require.NoError(t, db.Close())
				db = nil
				db, err = NewDB(name, tc.backend, dir)
				require.NoError(t, err)
				t.Cleanup(func() {
					if db != nil {
						require.NoError(t, db.Close())
					}
				})

				gotLatest, err = db.Get([]byte("s/latest"))
				require.NoError(t, err)
				require.Equal(t, latestValue, gotLatest, "s/latest reopen visibility")

				ok, err = db.Has(commitInfoKey)
				require.NoError(t, err)
				require.True(t, ok, "s/<height> reopen visibility")
			})
		})
	}
}
