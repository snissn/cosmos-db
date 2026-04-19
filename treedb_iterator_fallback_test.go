package db

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeKVIterator struct {
	start  []byte
	end    []byte
	keys   [][]byte
	vals   [][]byte
	idx    int
	closed bool
}

func (it *fakeKVIterator) Domain() (start, end []byte) { return it.start, it.end }

func (it *fakeKVIterator) Valid() bool {
	return !it.closed && it.idx >= 0 && it.idx < len(it.keys)
}

func (it *fakeKVIterator) Next() {
	if it.Valid() {
		it.idx++
	}
}

func (it *fakeKVIterator) Key() []byte   { return it.keys[it.idx] }
func (it *fakeKVIterator) Value() []byte { return it.vals[it.idx] }
func (it *fakeKVIterator) KeyCopy(dst []byte) []byte {
	key := it.Key()
	dst = append(dst[:0], key...)
	return dst
}
func (it *fakeKVIterator) ValueCopy(dst []byte) []byte {
	val := it.Value()
	dst = append(dst[:0], val...)
	return dst
}
func (it *fakeKVIterator) Error() error { return nil }
func (it *fakeKVIterator) Close() error {
	it.closed = true
	return nil
}

func TestBoundedKVIterator_EnforcesEndBound(t *testing.T) {
	src := &fakeKVIterator{
		keys: [][]byte{
			[]byte("a"),
			[]byte("b"),
			[]byte("c"),
			[]byte("d"),
		},
		vals: [][]byte{
			[]byte("va"),
			[]byte("vb"),
			[]byte("vc"),
			[]byte("vd"),
		},
	}

	it := newBoundedKVIterator([]byte("b"), []byte("d"), src)
	require.True(t, it.Valid())
	require.Equal(t, []byte("b"), it.Key())
	require.Equal(t, []byte("vb"), it.Value())

	it.Next()
	require.True(t, it.Valid())
	require.Equal(t, []byte("c"), it.Key())

	it.Next()
	require.False(t, it.Valid())
	require.NoError(t, it.Error())
}

func TestIsPrefixedIAVLVersionRange(t *testing.T) {
	prefix := []byte("s/k:minfee/")
	start := make([]byte, len(prefix)+9)
	copy(start, prefix)
	start[len(prefix)] = 's'
	binary.BigEndian.PutUint64(start[len(prefix)+1:], 1)

	end := make([]byte, len(prefix)+9)
	copy(end, prefix)
	end[len(prefix)] = 's'
	binary.BigEndian.PutUint64(end[len(prefix)+1:], uint64(math.MaxInt64))

	require.True(t, isPrefixedIAVLVersionRange(start, end))

	badEnd := append([]byte(nil), end...)
	binary.BigEndian.PutUint64(badEnd[len(prefix)+1:], 42)
	require.False(t, isPrefixedIAVLVersionRange(start, badEnd))
}
