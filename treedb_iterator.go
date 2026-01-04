package db

import "github.com/snissn/gomap/kvstore"

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
