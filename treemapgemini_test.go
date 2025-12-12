package db

import (
	"os"
	"testing"
)

// Basic smoke tests for the treemapgemini backend.
func TestTreeMapGeminiBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "treemapgemini-basic-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDB("treemapgemini-basic", TreeMapGeminiBackend, dir)
	if err != nil {
		t.Fatalf("new treemapgemini db: %v", err)
	}
	defer db.Close()

	k1 := []byte("a")
	v1 := []byte("alpha")
	k2 := []byte("b")
	v2 := []byte("bravo")

	if err := db.Set(k1, v1); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.SetSync(k2, v2); err != nil {
		t.Fatalf("setsync: %v", err)
	}

	got, err := db.Get(k1)
	if err != nil || string(got) != string(v1) {
		t.Fatalf("get k1: got %q err=%v", got, err)
	}
	exists, err := db.Has(k2)
	if err != nil || !exists {
		t.Fatalf("has k2: exists=%v err=%v", exists, err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	checkItem(t, it, k1, v1)
	it.Next()
	checkItem(t, it, k2, v2)
	it.Next()
	checkInvalid(t, it)

	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("rev iterator: %v", err)
	}
	defer rit.Close()
	checkItem(t, rit, k2, v2)
	rit.Next()
	checkItem(t, rit, k1, v1)
	rit.Next()
	checkInvalid(t, rit)

	if err := db.DeleteSync(k1); err != nil {
		t.Fatalf("deletesync: %v", err)
	}
	got, err = db.Get(k1)
	if err != nil || got != nil {
		t.Fatalf("deleted key returned %q err=%v", got, err)
	}
}

// Batch semantics should mirror other stores.
func TestTreeMapGeminiBatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "treemapgemini-batch-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDB("treemapgemini-batch", TreeMapGeminiBackend, dir)
	if err != nil {
		t.Fatalf("new treemapgemini db: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	if err := b.Set([]byte("1"), []byte("one")); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if err := b.Set([]byte("2"), []byte("two")); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if err := b.Delete([]byte("2")); err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	if size, err := b.GetByteSize(); err != nil || size == 0 {
		t.Fatalf("batch size: size=%d err=%v", size, err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch write: %v", err)
	}

	if v, _ := db.Get([]byte("1")); string(v) != "one" {
		t.Fatalf("expected one, got %q", v)
	}
	if v, _ := db.Get([]byte("2")); v != nil {
		t.Fatalf("expected nil for deleted key, got %q", v)
	}
}
