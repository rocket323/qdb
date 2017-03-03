// +build all bitcask

package bitcask

import (
    bcask "github.com/rocket323/bitcask"
)

type Iterator struct {
    db *BitCask
    err error
    iter *bcask.SnapshotIter
}

func newIterator(db *BitCask, snap *Snapshot) *Iterator {
    return &Iterator {
        db: db,
        iter: snap.snap.NewSnapshotIter(),
    }
}

func (it *Iterator) Close() {
    it.iter.Close()
}

func (it *Iterator) SeekToFirst() {
    it.iter.SeekToFirst()
}

func (it *Iterator) Valid() bool {
    return it.err == nil && it.iter.Valid()
}

func (it *Iterator) Next() {
    it.iter.Next()
}

func (it *Iterator) Key() []byte {
    return it.iter.Key()
}

func (it *Iterator) Value() []byte {
    return it.iter.Value()
}

func (it *Iterator) Error() error {
    return it.err
}

func (it *Iterator) SeekTo(key []byte) []byte {
    it.err = ErrNotSupported
    return nil
}

func (it *Iterator) SeekToLast() {
    it.err = ErrNotSupported
}

func (it *Iterator) Prev() {
    it.err = ErrNotSupported
}

