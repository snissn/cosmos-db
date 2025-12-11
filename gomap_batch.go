package db

// gomapBatch is a simple in-memory batch applied to the underlying tree on Write.
type gomapBatch struct {
	db   *GomapDB
	ops  []gomapOperation
	size int
}

type gomapOpType int

const (
	gomapOpSet gomapOpType = iota + 1
	gomapOpDelete
)

type gomapOperation struct {
	typ   gomapOpType
	key   []byte
	value []byte
}

var _ Batch = (*gomapBatch)(nil)

func newGomapBatch(db *GomapDB) *gomapBatch {
	return &gomapBatch{
		db:  db,
		ops: []gomapOperation{},
	}
}

// Set implements Batch.
func (b *gomapBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.size += len(key) + len(value)
	b.ops = append(b.ops, gomapOperation{typ: gomapOpSet, key: key, value: value})
	return nil
}

// Delete implements Batch.
func (b *gomapBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.size += len(key)
	b.ops = append(b.ops, gomapOperation{typ: gomapOpDelete, key: key})
	return nil
}

// Write implements Batch.
func (b *gomapBatch) Write() error {
	if b.ops == nil {
		return errBatchClosed
	}
	for _, op := range b.ops {
		switch op.typ {
		case gomapOpSet:
			if err := b.db.Set(op.key, op.value); err != nil {
				return err
			}
		case gomapOpDelete:
			if err := b.db.Delete(op.key); err != nil {
				return err
			}
		}
	}
	return b.Close()
}

// WriteSync implements Batch.
func (b *gomapBatch) WriteSync() error {
	return b.Write()
}

// Close implements Batch.
func (b *gomapBatch) Close() error {
	b.ops = nil
	b.size = 0
	return nil
}

// GetByteSize implements Batch.
func (b *gomapBatch) GetByteSize() (int, error) {
	if b.ops == nil {
		return 0, errBatchClosed
	}
	return b.size, nil
}
