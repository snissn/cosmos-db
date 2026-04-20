package db

import (
	"bytes"

	"github.com/snissn/gomap/kvstore"
)

type keyArena struct {
	buf []byte
}

func newKeyArena(capacity int) keyArena {
	if capacity <= 0 {
		capacity = 64 * 1024
	}
	return keyArena{buf: make([]byte, 0, capacity)}
}

func (a *keyArena) Copy(key []byte) ([]byte, bool) {
	if len(key) > cap(a.buf)-len(a.buf) {
		return nil, false
	}
	off := len(a.buf)
	a.buf = append(a.buf, key...)
	return a.buf[off : off+len(key)], true
}

type coreIterator struct {
	iter  kvstore.Iterator
	start []byte
	end   []byte

	keyArena keyArena
	valArena keyArena
}

var _ Iterator = (*coreIterator)(nil)

// Domain implements Iterator.
func (it *coreIterator) Domain() (start, end []byte) { return it.start, it.end }

// Valid implements Iterator.
func (it *coreIterator) Valid() bool { return it.iter.Valid() }

// Next implements Iterator.
func (it *coreIterator) Next() {
	it.assertIsValid()
	it.iter.Next()
}

// Key implements Iterator.
func (it *coreIterator) Key() []byte {
	it.assertIsValid()
	if it.keyArena.buf == nil {
		it.keyArena = newKeyArena(64 * 1024)
	}
	key := it.iter.Key()
	out, ok := it.keyArena.Copy(key)
	if ok {
		return out
	}
	out = make([]byte, len(key))
	copy(out, key)
	return out
}

// Value implements Iterator.
func (it *coreIterator) Value() []byte {
	it.assertIsValid()
	if it.valArena.buf == nil {
		it.valArena = newKeyArena(256 * 1024)
	}
	val := it.iter.Value()
	out, ok := it.valArena.Copy(val)
	if ok {
		return out
	}
	out = make([]byte, len(val))
	copy(out, val)
	return out
}

// Error implements Iterator.
func (it *coreIterator) Error() error { return it.iter.Error() }

// Close implements Iterator.
func (it *coreIterator) Close() error { return it.iter.Close() }

func (it *coreIterator) assertIsValid() {
	if !it.Valid() {
		panic("iterator is invalid")
	}
}

// boundedKVIterator wraps a source iterator and enforces [start,end) bounds
// locally. This is used as a safety fallback when backend bounded iteration
// for IAVL version ranges yields false-empty results.
type boundedKVIterator struct {
	src   kvstore.Iterator
	start []byte
	end   []byte
	valid bool
}

func newBoundedKVIterator(start, end []byte, src kvstore.Iterator) *boundedKVIterator {
	it := &boundedKVIterator{
		src:   src,
		start: start,
		end:   end,
	}
	it.seek()
	return it
}

func (it *boundedKVIterator) Domain() (start, end []byte) { return it.start, it.end }

func (it *boundedKVIterator) Valid() bool {
	return it != nil && it.src != nil && it.valid && it.src.Valid()
}

func (it *boundedKVIterator) Next() {
	it.assertIsValid()
	it.src.Next()
	it.seek()
}

func (it *boundedKVIterator) Key() []byte {
	it.assertIsValid()
	return it.src.Key()
}

func (it *boundedKVIterator) Value() []byte {
	it.assertIsValid()
	return it.src.Value()
}

func (it *boundedKVIterator) KeyCopy(dst []byte) []byte {
	it.assertIsValid()
	return it.src.KeyCopy(dst)
}

func (it *boundedKVIterator) ValueCopy(dst []byte) []byte {
	it.assertIsValid()
	return it.src.ValueCopy(dst)
}

func (it *boundedKVIterator) Error() error {
	if it == nil || it.src == nil {
		return nil
	}
	return it.src.Error()
}

func (it *boundedKVIterator) Close() error {
	if it == nil || it.src == nil {
		return nil
	}
	return it.src.Close()
}

func (it *boundedKVIterator) assertIsValid() {
	if !it.Valid() {
		panic("iterator is invalid")
	}
}

func (it *boundedKVIterator) seek() {
	it.valid = false
	if it == nil || it.src == nil {
		return
	}
	for it.src.Valid() {
		key := it.src.Key()
		if it.start != nil && bytes.Compare(key, it.start) < 0 {
			it.src.Next()
			continue
		}
		if it.end != nil && bytes.Compare(key, it.end) >= 0 {
			return
		}
		it.valid = true
		return
	}
}

func (d *TreeDB) forwardIteratorWithIAVLFallback(start, end []byte) (kvstore.Iterator, error) {
	it, err := d.kv.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	if !isPrefixedIAVLVersionRange(start, end) {
		return it, nil
	}
	if it.Valid() || it.Error() != nil {
		return it, nil
	}
	_ = it.Close()

	// Fallback: iterate from start with an open upper bound and enforce the
	// original end bound locally.
	alt, err := d.kv.Iterator(start, nil)
	if err != nil {
		return nil, err
	}
	bounded := newBoundedKVIterator(start, end, alt)
	if bounded.Valid() || bounded.Error() != nil {
		if treedbVisibilityOn() {
			treedbVisibilityf(
				"iter fallback iavl_range=true mode=open_end start=%x end=%x alt_valid=%t",
				start, end, bounded.Valid(),
			)
		}
		return bounded, nil
	}

	// If the backend's start bound is also pathological, fall back to prefix
	// iteration and enforce both bounds locally.
	prefix := start[:len(start)-9]
	prefixEnd := cpIncr(prefix)
	_ = bounded.Close()
	alt2, err := d.kv.Iterator(prefix, prefixEnd)
	if err != nil {
		return nil, err
	}
	bounded2 := newBoundedKVIterator(start, end, alt2)
	if treedbVisibilityOn() {
		treedbVisibilityf(
			"iter fallback iavl_range=true mode=prefix_scan start=%x end=%x alt_valid=%t",
			start, end, bounded2.Valid(),
		)
	}
	return bounded2, nil
}
