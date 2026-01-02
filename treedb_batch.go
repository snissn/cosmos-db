package db

import "github.com/snissn/gomap/kvstore"

type coreBatch struct {
	db         *TreeDB
	kb         kvstore.Batch
	setView    func(key, value []byte) error
	deleteView func(key []byte) error
	size       int
	done       bool
}

var _ Batch = (*coreBatch)(nil)

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
	if b.setView != nil {
		if err := b.setView(key, value); err != nil {
			return err
		}
	} else if err := b.kb.Set(key, value); err != nil {
		return err
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
	if b.deleteView != nil {
		if err := b.deleteView(key); err != nil {
			return err
		}
	} else if err := b.kb.Delete(key); err != nil {
		return err
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
	return b.kb.Commit()
}

// WriteSync implements Batch.
func (b *coreBatch) WriteSync() error {
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	b.done = true
	return b.kb.CommitSync()
}

// Close implements Batch.
func (b *coreBatch) Close() error {
	if b.kb == nil {
		b.done = true
		return nil
	}
	err := b.kb.Close()
	b.kb = nil
	b.done = true
	return err
}

// GetByteSize implements Batch.
func (b *coreBatch) GetByteSize() (int, error) {
	if b.done || b.kb == nil {
		return 0, errBatchClosed
	}
	return b.size, nil
}
