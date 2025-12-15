package db

import (
	"errors"

	treedb "github.com/snissn/gomap/TreeDB"
)

type treeDBBatchOpType uint8

const (
	treeDBBatchOpSet treeDBBatchOpType = iota
	treeDBBatchOpDelete
)

type treeDBBatchOp struct {
	typ   treeDBBatchOpType
	key   []byte
	value []byte
}

type treeDBBatch struct {
	db *TreeDB

	ops      []treeDBBatchOp
	byteSize int
	closed   bool
}

var _ Batch = (*treeDBBatch)(nil)

func newTreeDBBatch(db *TreeDB, sizeHint int) *treeDBBatch {
	var ops []treeDBBatchOp
	if sizeHint > 0 && sizeHint <= 1024 {
		ops = make([]treeDBBatchOp, 0, sizeHint)
	}
	return &treeDBBatch{
		db:  db,
		ops: ops,
	}
}

func (b *treeDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.closed {
		return errBatchClosed
	}

	b.ops = append(b.ops, treeDBBatchOp{typ: treeDBBatchOpSet, key: key, value: value})
	b.byteSize += len(key) + len(value)
	return nil
}

func (b *treeDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.closed {
		return errBatchClosed
	}

	b.ops = append(b.ops, treeDBBatchOp{typ: treeDBBatchOpDelete, key: key})
	b.byteSize += len(key)
	return nil
}

func (b *treeDBBatch) Write() error {
	return b.write(false)
}

func (b *treeDBBatch) WriteSync() error {
	return b.write(true)
}

func (b *treeDBBatch) write(sync bool) error {
	if b.closed {
		return errBatchClosed
	}
	if b.db == nil || b.db.db == nil {
		return errors.New("treedb: db is closed")
	}

	tb := b.db.db.NewBatch()
	if tb == nil {
		return errors.New("treedb: failed to create batch (db closed)")
	}
	defer tb.Close()

	for _, op := range b.ops {
		switch op.typ {
		case treeDBBatchOpSet:
			if err := tb.Set(op.key, op.value); err != nil {
				return err
			}
		case treeDBBatchOpDelete:
			if err := tb.Delete(op.key); err != nil {
				return err
			}
		default:
			return errors.New("treedb: unknown batch op")
		}
	}

	var err error
	if sync {
		err = tb.WriteSync()
	} else {
		err = tb.Write()
	}
	if err != nil {
		return err
	}

	return b.Close()
}

func (b *treeDBBatch) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	b.ops = nil
	b.byteSize = 0
	return nil
}

func (b *treeDBBatch) GetByteSize() (int, error) {
	if b.closed {
		return 0, errBatchClosed
	}
	return b.byteSize, nil
}

var _ treedb.Batch = (*treeDBBatch)(nil)
