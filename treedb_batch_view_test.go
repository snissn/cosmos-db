package db

import (
	"testing"

	"github.com/snissn/gomap/kvstore"
)

type stubKVBatch struct {
	setCount        int
	setViewCount    int
	deleteCount     int
	deleteViewCount int
}

func (b *stubKVBatch) Set(key, value []byte) error {
	b.setCount++
	return nil
}

func (b *stubKVBatch) Delete(key []byte) error {
	b.deleteCount++
	return nil
}

func (b *stubKVBatch) Commit() error     { return nil }
func (b *stubKVBatch) CommitSync() error { return nil }
func (b *stubKVBatch) Close() error      { return nil }

func (b *stubKVBatch) SetView(key, value []byte) error {
	b.setViewCount++
	return nil
}

func (b *stubKVBatch) DeleteView(key []byte) error {
	b.deleteViewCount++
	return nil
}

var _ kvstore.Batch = (*stubKVBatch)(nil)

func TestCoreBatchSetView_ForwardsWhenAvailable(t *testing.T) {
	stub := &stubKVBatch{}
	b := &coreBatch{kb: stub}
	if err := b.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("setview: %v", err)
	}
	if stub.setViewCount != 1 || stub.setCount != 0 {
		t.Fatalf("expected SetView forwarding, set=%d setview=%d", stub.setCount, stub.setViewCount)
	}
}

func TestCoreBatchDeleteView_ForwardsWhenAvailable(t *testing.T) {
	stub := &stubKVBatch{}
	b := &coreBatch{kb: stub}
	if err := b.DeleteView([]byte("k")); err != nil {
		t.Fatalf("deleteview: %v", err)
	}
	if stub.deleteViewCount != 1 || stub.deleteCount != 0 {
		t.Fatalf("expected DeleteView forwarding, delete=%d deleteview=%d", stub.deleteCount, stub.deleteViewCount)
	}
}

func TestPrefixBatchSetView_ForwardsWhenAvailable(t *testing.T) {
	stub := &coreBatch{kb: &stubKVBatch{}}
	pb := newPrefixBatch([]byte("p/"), stub)
	if err := pb.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("setview: %v", err)
	}
	got := stub.kb.(*stubKVBatch)
	if got.setViewCount != 1 || got.setCount != 0 {
		t.Fatalf("expected prefixed SetView forwarding, set=%d setview=%d", got.setCount, got.setViewCount)
	}
}
