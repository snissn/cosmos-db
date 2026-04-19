package db

import "github.com/snissn/gomap/kvstore"

type coreBatch struct {
	db   *TreeDB
	kb   kvstore.Batch
	size int
	done bool
}

var _ Batch = (*coreBatch)(nil)

type batchSetViewer interface {
	SetView(key, value []byte) error
}

type batchDeleteViewer interface {
	DeleteView(key []byte) error
}

// Set implements Batch.
func (b *coreBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	if err := b.kb.Set(key, value); err != nil {
		return err
	}
	b.size += len(key) + len(value)
	return nil
}

// SetView records a Put without forcing another key/value copy when the
// underlying kv batch supports view semantics. Callers must keep key/value
// immutable until Write/WriteSync/Close.
func (b *coreBatch) SetView(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	if sv, ok := b.kb.(batchSetViewer); ok {
		if err := sv.SetView(key, value); err != nil {
			return err
		}
	} else {
		if err := b.kb.Set(key, value); err != nil {
			return err
		}
	}
	b.size += len(key) + len(value)
	return nil
}

// Delete implements Batch.
func (b *coreBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	if err := b.kb.Delete(key); err != nil {
		return err
	}
	b.size += len(key)
	return nil
}

// DeleteView records a Delete without forcing another key copy when the
// underlying kv batch supports view semantics. Callers must keep key immutable
// until Write/WriteSync/Close.
func (b *coreBatch) DeleteView(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	if dv, ok := b.kb.(batchDeleteViewer); ok {
		if err := dv.DeleteView(key); err != nil {
			return err
		}
	} else {
		if err := b.kb.Delete(key); err != nil {
			return err
		}
	}
	b.size += len(key)
	return nil
}

// Write implements Batch.
func (b *coreBatch) Write() error {
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	b.done = true
	if b.db != nil {
		return b.db.withSerializedBatchWrite(func() error {
			if err := b.kb.Commit(); err != nil {
				return err
			}
			if b.db.forceCheckpointOnWrite {
				if err := b.db.writeSyncBarrier(); err != nil {
					return err
				}
			}
			return b.db.maybeCheckpointAfterWrite()
		})
	}
	if err := b.kb.Commit(); err != nil {
		return err
	}
	return nil
}

// WriteSync implements Batch.
func (b *coreBatch) WriteSync() error {
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	b.done = true
	if b.db != nil {
		return b.db.withSerializedBatchWrite(func() error {
			if err := b.kb.CommitSync(); err != nil {
				return err
			}
			return b.db.writeSyncBarrier()
		})
	}
	if err := b.kb.CommitSync(); err != nil {
		return err
	}
	return nil
}

// Close implements Batch.
func (b *coreBatch) Close() error {
	if b.kb == nil {
		b.done = true
		return nil
	}

	alreadyDone := b.done
	err := b.kb.Close()
	b.kb = nil
	b.done = true
	// Close is expected to be idempotent, and callers like IAVL's
	// BatchWithFlusher call Close() after Write()/WriteSync(). If the batch
	// was already written, do not surface close-time errors.
	if alreadyDone {
		return nil
	}
	if err != nil {
		return err
	}

	return err
}

// GetByteSize implements Batch.
func (b *coreBatch) GetByteSize() (int, error) {
	if b.done || b.kb == nil {
		return 0, errBatchClosed
	}
	return b.size, nil
}
