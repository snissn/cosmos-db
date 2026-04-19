package db

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	gogotypes "github.com/cosmos/gogoproto/types"
	"github.com/stretchr/testify/require"
)

// Reproduces the state-sync restore pattern where many prefixed IAVL node
// writes are followed by a root-store metadata flush (`s/<height>` and
// `s/latest`). The metadata keys must be visible immediately and survive reopen.
func TestTreeDBMetadataKeysVisibleAfterHeavyPrefixedWrites(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()

	db, err := NewDB(name, TreeDBBackend, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		cleanupDBDir(dir, name)
	})

	stores := []string{
		"acc", "authz", "bank", "blob", "capability",
		"circuit", "consensus", "distribution", "evidence", "feegrant",
		"gov", "hyperlane", "ibc", "icahost", "minfee",
		"mint", "packetfowardmiddleware", "params", "signal", "slashing",
		"staking", "transfer", "upgrade", "warp",
	}
	const height = uint64(9_993_000)
	const keysPerStore = 4096

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
	require.Equal(t, latestValue, gotLatest, "s/latest should be visible immediately after WriteSync")

	ok, err := db.Has(commitInfoKey)
	require.NoError(t, err)
	require.True(t, ok, "s/<height> commit-info key should be visible immediately after WriteSync")

	require.NoError(t, db.Close())

	db, err = NewDB(name, TreeDBBackend, dir)
	require.NoError(t, err)

	gotLatest, err = db.Get([]byte("s/latest"))
	require.NoError(t, err)
	require.Equal(t, latestValue, gotLatest, "s/latest should survive reopen")
}
