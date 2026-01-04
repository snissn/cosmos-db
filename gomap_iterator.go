//go:build gomap
// +build gomap

package db

import gbtree "github.com/snissn/gomap/btree"

// gomapIterator wraps a btree iterator (forward or reverse) to satisfy the DB Iterator interface.
type gomapIterator struct {
	start   []byte
	end     []byte
	iter    *gbtree.Iter
	revIter *gbtree.RevIter
}

var _ Iterator = (*gomapIterator)(nil)

// Domain implements Iterator.
func (it *gomapIterator) Domain() (start, end []byte) {
	return it.start, it.end
}

// Valid implements Iterator.
func (it *gomapIterator) Valid() bool {
	if it.iter != nil {
		return it.iter.Valid()
	}
	return it.revIter.Valid()
}

// Next implements Iterator.
func (it *gomapIterator) Next() {
	it.assertValid()
	if it.iter != nil {
		it.iter.Next()
		return
	}
	it.revIter.Next()
}

// Key implements Iterator.
func (it *gomapIterator) Key() (key []byte) {
	it.assertValid()
	if it.iter != nil {
		return it.iter.Key()
	}
	return it.revIter.Key()
}

// Value implements Iterator.
func (it *gomapIterator) Value() (value []byte) {
	it.assertValid()
	if it.iter != nil {
		return it.iter.Value()
	}
	return it.revIter.Value()
}

// Error implements Iterator.
func (it *gomapIterator) Error() error {
	if it.iter != nil {
		return it.iter.Error()
	}
	return it.revIter.Error()
}

// Close implements Iterator.
func (it *gomapIterator) Close() error {
	if it.iter != nil {
		it.iter.Close()
		return nil
	}
	it.revIter.Close()
	return nil
}

func (it *gomapIterator) assertValid() {
	if !it.Valid() {
		panic("iterator is invalid")
	}
}
