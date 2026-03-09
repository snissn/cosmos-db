package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Batch.Set/Delete must preserve standard copy semantics: callers may reuse or
// mutate key/value buffers after Set/Delete returns and before Write/WriteSync.
func TestTreeDBBatchSetCopiesInputBuffers(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, TreeDBBackend, dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		cleanupDBDir(dir, name)
	})

	batch := db.NewBatch()
	t.Cleanup(func() { _ = batch.Close() })

	const n = 64
	keyBuf := make([]byte, 4)
	valBuf := make([]byte, 16)

	want := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		copy(keyBuf, []byte(fmt.Sprintf("k%03d", i)))
		fill := byte(i)
		for j := range valBuf {
			valBuf[j] = fill
		}
		require.NoError(t, batch.Set(keyBuf, valBuf))

		// Persist expected content independently of subsequent caller mutation.
		k := string(append([]byte(nil), keyBuf...))
		v := append([]byte(nil), valBuf...)
		want[k] = v
	}

	// Mutate caller-owned buffers before commit; this must not affect batch
	// contents.
	copy(keyBuf, []byte("zzzz"))
	for i := range valBuf {
		valBuf[i] = 0xFF
	}

	require.NoError(t, batch.Write())

	for k, v := range want {
		got, err := db.Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, v, got, "key=%s", k)
	}
}
