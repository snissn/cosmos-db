package db

type prefixDBBatch struct {
	prefix []byte
	source Batch
}

var _ Batch = (*prefixDBBatch)(nil)

type prefixBatchSetViewer interface {
	SetView(key, value []byte) error
}

type prefixBatchDeleteViewer interface {
	DeleteView(key []byte) error
}

func newPrefixBatch(prefix []byte, source Batch) prefixDBBatch {
	return prefixDBBatch{
		prefix: prefix,
		source: source,
	}
}

// Set implements Batch.
func (pb prefixDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	pkey := append(cp(pb.prefix), key...)
	return pb.source.Set(pkey, value)
}

// SetView preserves view semantics through prefix wrapping when the underlying
// batch supports it. The prefixed key is still owned by this wrapper.
func (pb prefixDBBatch) SetView(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	pkey := append(cp(pb.prefix), key...)
	if sv, ok := pb.source.(prefixBatchSetViewer); ok {
		return sv.SetView(pkey, value)
	}
	return pb.source.Set(pkey, value)
}

// Delete implements Batch.
func (pb prefixDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	pkey := append(cp(pb.prefix), key...)
	return pb.source.Delete(pkey)
}

// DeleteView preserves view semantics through prefix wrapping when the
// underlying batch supports it. The prefixed key is still owned by this wrapper.
func (pb prefixDBBatch) DeleteView(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	pkey := append(cp(pb.prefix), key...)
	if dv, ok := pb.source.(prefixBatchDeleteViewer); ok {
		return dv.DeleteView(pkey)
	}
	return pb.source.Delete(pkey)
}

// Write implements Batch.
func (pb prefixDBBatch) Write() error {
	return pb.source.Write()
}

// WriteSync implements Batch.
func (pb prefixDBBatch) WriteSync() error {
	return pb.source.WriteSync()
}

// Close implements Batch.
func (pb prefixDBBatch) Close() error {
	return pb.source.Close()
}

// GetByteSize implements Batch
func (pb prefixDBBatch) GetByteSize() (int, error) {
	if pb.source == nil {
		return 0, errBatchClosed
	}
	return pb.source.GetByteSize()
}
