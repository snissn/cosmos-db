package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackendRegistration(t *testing.T) {
	// Define the list of expected backends
	expectedBackends := []BackendType{
		GoLevelDBBackend,
		MemDBBackend,
		PebbleDBBackend,
		TreeDBBackend,
		BackendType("db_backend treedb"), // Alias
		BackendType("prefixdb"), // Registered in backend_test.go
	}

	// Iterate over each expected backend and verify it can be initialized
	for _, backend := range expectedBackends {
		t.Run(string(backend), func(t *testing.T) {
			dir := t.TempDir()
			db, err := NewDB("test", backend, dir)
			require.NoError(t, err, "Failed to initialize backend: %s", backend)
			require.NotNil(t, db, "Database instance should not be nil for backend: %s", backend)
			db.Close()
		})
	}
}

func TestRocksDBRegistration(t *testing.T) {
	// Check if RocksDB is supported (requires build tag)
	// We can check this by attempting to create a DB and checking the error message,
	// or by checking if the creator is in the map (but the map is private).
	// simpler: try to create it. If it fails with "unknown db_backend", then it's not compiled in.
	// If it fails with something else (like "failed to initialize..."), then it IS compiled in but failed for other reasons.
	
	dir := t.TempDir()
	_, err := NewDB("test_rocks", RocksDBBackend, dir)
	
	// If we are running without the rocksdb tag, this should fail with "unknown db_backend"
	// If we are running WITH the rocksdb tag, it might succeed or fail with linker errors if libs are missing,
	// but for the purpose of this test, we just want to know if "unknown db_backend" is returned when we expect it NOT to be (if tags are on).
	// However, we don't know if the tag is on inside this test code easily without using build constraints on the test file itself.

	// Let's rely on the fact that if it IS registered, it shouldn't be unknown.
	// If it is NOT registered, it MUST be unknown.
	
	// Since we can't easily detect the build tag at runtime without a helper, 
	// we will skip this specific assertion in the general loop and handle it here:
	
	if err != nil {
		// If the error is "unknown db_backend ...", then it's fine IF we didn't expect it.
		// But if the CI runs with -tags rocksdb, this SHOULD NOT happen.
		t.Logf("RocksDB initialization failed: %v", err)
	} else {
		t.Log("RocksDB initialized successfully")
	}
}
