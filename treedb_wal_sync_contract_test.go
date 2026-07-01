package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTreeDBWriteSyncDurableAndSnapshotVisibleWithoutCheckpoint(t *testing.T) {
	t.Setenv(envTreeDBOpenProfile, "command_wal_durable")

	name := fmt.Sprintf("test_%x", randStr(12))
	dir := t.TempDir()

	db, err := NewDB(name, TreeDBBackend, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		if db != nil {
			require.NoError(t, db.Close())
		}
	})

	b := db.NewBatch()
	require.NoError(t, b.Set([]byte("k1"), []byte("v1")))
	require.NoError(t, b.WriteSync())
	require.NoError(t, b.Close())

	got, err := db.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)

	tdb, ok := db.(*TreeDB)
	require.True(t, ok)
	tdb.PinSnapshot()
	got, err = db.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)
	tdb.UnpinSnapshot()

	require.NoError(t, db.Close())
	db = nil

	db, err = NewDB(name, TreeDBBackend, dir)
	require.NoError(t, err)

	got, err = db.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)
}

func TestTreeDBAdapterRejectsNonDurableProfile(t *testing.T) {
	t.Setenv(envTreeDBOpenProfile, "bench")

	db, err := NewDB(fmt.Sprintf("test_%x", randStr(12)), TreeDBBackend, t.TempDir())
	require.Error(t, err)
	require.Nil(t, db)
	require.Contains(t, err.Error(), "supports only")
}

func BenchmarkTinyBatchWriteSync(b *testing.B) {
	cases := []struct {
		name    string
		backend BackendType
		profile string
	}{
		{name: "goleveldb", backend: GoLevelDBBackend},
		{name: "treedb_command_wal_durable", backend: TreeDBBackend, profile: "command_wal_durable"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			if tc.backend == TreeDBBackend {
				b.Setenv(envTreeDBOpenProfile, tc.profile)
			}
			name := fmt.Sprintf("bench_%x_%s", randStr(12), tc.name)
			db, err := NewDB(name, tc.backend, b.TempDir())
			require.NoError(b, err)
			b.Cleanup(func() {
				require.NoError(b, db.Close())
			})

			key := []byte("tiny-batch-key")
			value := []byte("tiny-batch-value")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch()
				require.NoError(b, batch.Set(key, value))
				require.NoError(b, batch.WriteSync())
				require.NoError(b, batch.Close())
			}
		})
	}
}
