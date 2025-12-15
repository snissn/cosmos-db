package db

import treedb "github.com/snissn/gomap/TreeDB"

type treeDBIterator struct {
	source  treedb.Iterator
	start   []byte
	end     []byte
	invalid bool
}

var _ Iterator = (*treeDBIterator)(nil)

func newTreeDBIterator(source treedb.Iterator, start, end []byte) *treeDBIterator {
	return &treeDBIterator{
		source: source,
		start:  start,
		end:    end,
	}
}

func (itr *treeDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *treeDBIterator) Valid() bool {
	if itr.invalid {
		return false
	}
	if itr.source == nil {
		itr.invalid = true
		return false
	}
	if err := itr.source.Error(); err != nil {
		itr.invalid = true
		return false
	}
	if !itr.source.Valid() {
		itr.invalid = true
		return false
	}
	return true
}

func (itr *treeDBIterator) Next() {
	itr.assertIsValid()
	itr.source.Next()
}

func (itr *treeDBIterator) Key() []byte {
	itr.assertIsValid()
	key := itr.source.Key()
	if key == nil {
		return nil
	}
	return cp(key)
}

func (itr *treeDBIterator) Value() []byte {
	itr.assertIsValid()
	value := itr.source.Value()
	if value == nil {
		return nil
	}
	return cp(value)
}

func (itr *treeDBIterator) Error() error {
	if itr.source == nil {
		return nil
	}
	return itr.source.Error()
}

func (itr *treeDBIterator) Close() error {
	if itr.source == nil {
		return nil
	}
	err := itr.source.Close()
	itr.source = nil
	itr.invalid = true
	return err
}

func (itr *treeDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
