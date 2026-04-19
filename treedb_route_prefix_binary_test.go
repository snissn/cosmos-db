package db

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTreeDBRouteMode_PrefixedBinaryRootsSurviveWriteSync(t *testing.T) {
	cases := []struct {
		name      string
		writeSync bool
	}{
		{name: "write_sync", writeSync: true},
		{name: "write", writeSync: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			name := fmt.Sprintf("test_%x", randStr(12))
			dir := os.TempDir()

			db, err := NewDB(name, TreeDBBackend, dir)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, db.Close())
				cleanupDBDir(dir, name)
			})

			acc := NewPrefixDB(db, []byte("s/k:acc/"))
			authz := NewPrefixDB(db, []byte("s/k:authz/"))

			const targetVersion = int64(9988000)
			const olderVersion = int64(9987746)

			accRootValue := bytes.Repeat([]byte{0xA1}, 73)
			authzOldRootValue := bytes.Repeat([]byte{0xB2}, 122)
			authzRootValue := bytes.Repeat([]byte{0xC3}, 13)

			writeStoreBatch := func(store *PrefixDB, startVersion int64, count int, rootVersion int64, rootValue []byte, extraRootVersion int64, extraRootValue []byte) {
				b := store.NewBatch()
				for i := 0; i < count; i++ {
					version := startVersion + int64(i)
					key := iavlNodeKey(version, uint32((i%31)+2)) // nonce 1 is reserved for root
					val := bytes.Repeat([]byte{byte((i % 251) + 1)}, 96)
					require.NoError(t, b.Set(key, val))
				}
				require.NoError(t, b.Set(iavlNodeKey(rootVersion, 1), rootValue))
				if extraRootVersion > 0 {
					require.NoError(t, b.Set(iavlNodeKey(extraRootVersion, 1), extraRootValue))
				}
				if tc.writeSync {
					require.NoError(t, b.WriteSync())
				} else {
					require.NoError(t, b.Write())
				}
				require.NoError(t, b.Close())
			}

			// Mirrors the sequence seen during state-sync restore logs:
			// first one store root commit, then another store batch with two nearby roots.
			writeStoreBatch(acc, targetVersion-9400, 9400, targetVersion, accRootValue, 0, nil)
			got, err := acc.Get(iavlNodeKey(targetVersion, 1))
			require.NoError(t, err)
			require.Equal(t, accRootValue, got)

			writeStoreBatch(authz, targetVersion-5560, 5560, olderVersion, authzOldRootValue, targetVersion, authzRootValue)
			got, err = authz.Get(iavlNodeKey(targetVersion, 1))
			require.NoError(t, err)
			require.Equal(t, authzRootValue, got)
		})
	}
}
