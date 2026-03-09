package db_test

import (
	"bytes"
	"math/rand"
	"testing"

	db "github.com/cosmos/cosmos-db"
	iavl "github.com/cosmos/iavl"
	idbm "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/require"
)

const (
	envTreeDBOpenProfile          = "TREEDB_OPEN_PROFILE"
	envTreeDBForceCheckpointWrite = "TREEDB_FORCE_CHECKPOINT_ON_WRITE"
)

type backendVisibilityCase struct {
	name    string
	backend db.BackendType
	profile string
}

type cosmosDBWrapper struct {
	db.DB
}

func (dbw *cosmosDBWrapper) Iterator(start, end []byte) (idbm.Iterator, error) {
	return dbw.DB.Iterator(start, end)
}

func (dbw *cosmosDBWrapper) ReverseIterator(start, end []byte) (idbm.Iterator, error) {
	return dbw.DB.ReverseIterator(start, end)
}

func (dbw *cosmosDBWrapper) NewBatch() idbm.Batch {
	return dbw.DB.NewBatch()
}

func (dbw *cosmosDBWrapper) NewBatchWithSize(size int) idbm.Batch {
	return dbw.DB.NewBatchWithSize(size)
}

func runWithBackendVisibilityEnv(t *testing.T, tc backendVisibilityCase, fn func()) {
	t.Helper()
	if tc.backend == db.TreeDBBackend {
		t.Setenv(envTreeDBOpenProfile, tc.profile)
		t.Setenv(envTreeDBForceCheckpointWrite, "0")
	}
	fn()
}

func openImporterBackendDB(t *testing.T, tc backendVisibilityCase, name, dir string) db.DB {
	t.Helper()
	if tc.backend == db.TreeDBBackend {
		t.Setenv(envTreeDBOpenProfile, tc.profile)
		t.Setenv(envTreeDBForceCheckpointWrite, "0")
	}
	out, err := db.NewDB(name, tc.backend, dir)
	require.NoError(t, err)
	return out
}

func buildIAVLExportTreeWithSamples(t *testing.T, treeSize int) (*iavl.ImmutableTree, [][]byte, map[string][]byte) {
	t.Helper()

	const (
		randSeed  = 49872768940
		keySize   = 16
		valueSize = 2048
	)

	r := rand.New(rand.NewSource(randSeed))
	tree := iavl.NewMutableTree(idbm.NewMemDB(), 0, false, iavl.NewNopLogger())
	sampleKeys := make([][]byte, 0, 8)
	sampleValues := make(map[string][]byte, 8)

	for i := 0; i < treeSize; i++ {
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		r.Read(key)
		r.Read(value)
		updated, err := tree.Set(key, value)
		require.NoError(t, err)
		if updated {
			i--
			continue
		}
		if len(sampleKeys) < cap(sampleKeys) {
			keyCopy := append([]byte(nil), key...)
			valueCopy := append([]byte(nil), value...)
			sampleKeys = append(sampleKeys, keyCopy)
			sampleValues[string(keyCopy)] = valueCopy
		}
	}

	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	itree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	return itree, sampleKeys, sampleValues
}

func exportIAVLNodes(t *testing.T, tree *iavl.ImmutableTree) []*iavl.ExportNode {
	t.Helper()
	exporter, err := tree.Export()
	require.NoError(t, err)
	defer exporter.Close()

	nodes := make([]*iavl.ExportNode, 0, 16384)
	for {
		node, err := exporter.Next()
		if err == iavl.ErrorExportDone {
			break
		}
		require.NoError(t, err)
		nodes = append(nodes, node)
	}
	return nodes
}

func TestBackendImporterLoadVersionParity(t *testing.T) {
	const importVersion = int64(1)
	const treeSize = 10000*2 + 257
	sourceTree, sampleKeys, sampleValues := buildIAVLExportTreeWithSamples(t, treeSize)
	exported := exportIAVLNodes(t, sourceTree)

	cases := []backendVisibilityCase{
		{name: "goleveldb", backend: db.GoLevelDBBackend},
		{name: "treedb_fast", backend: db.TreeDBBackend, profile: "fast"},
		{name: "treedb_wal_on_fast", backend: db.TreeDBBackend, profile: "wal_on_fast"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWithBackendVisibilityEnv(t, tc, func() {
				name := "testdb"
				dir := t.TempDir()
				database := openImporterBackendDB(t, tc, name, dir)
				t.Cleanup(func() {
					if database != nil {
						require.NoError(t, database.Close())
					}
				})

				prefix := []byte("s/k:staking/")
				importDB := db.NewPrefixDB(database, prefix)
				tree := iavl.NewMutableTree(&cosmosDBWrapper{DB: importDB}, 0, false, iavl.NewNopLogger())
				importer, err := tree.Import(importVersion)
				require.NoError(t, err)
				for _, node := range exported {
					require.NoError(t, importer.Add(node))
				}
				require.NoError(t, importer.Commit())
				importer.Close()

				fresh := iavl.NewMutableTree(&cosmosDBWrapper{DB: db.NewPrefixDB(database, prefix)}, 0, false, iavl.NewNopLogger())
				loaded, err := fresh.LoadVersion(importVersion)
				require.NoError(t, err)
				require.Equal(t, importVersion, loaded)

				for _, key := range sampleKeys {
					got, err := fresh.Get(key)
					require.NoError(t, err)
					require.True(t, bytes.Equal(sampleValues[string(key)], got), "same-handle key mismatch")
				}

				require.NoError(t, database.Close())
				database = nil
				database = openImporterBackendDB(t, tc, name, dir)

				reopened := iavl.NewMutableTree(&cosmosDBWrapper{DB: db.NewPrefixDB(database, prefix)}, 0, false, iavl.NewNopLogger())
				loaded, err = reopened.LoadVersion(importVersion)
				require.NoError(t, err)
				require.Equal(t, importVersion, loaded)

				for _, key := range sampleKeys {
					got, err := reopened.Get(key)
					require.NoError(t, err)
					require.True(t, bytes.Equal(sampleValues[string(key)], got), "reopen key mismatch")
				}
			})
		})
	}
}
