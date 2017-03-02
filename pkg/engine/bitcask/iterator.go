// +build all bitcask

package bitcask

import (
    "github.com/juju/errors"
    "github.com/reborndb/qdb/pkg/engine"
    bcask "github.com/rocket323/bitcask"
)

type Iterator struct {
    db *BCask
    err error
    iter *bcask.Iterator
}

func newIterator(db *BitCask, snap *Snapshot) *Iterator {
    return &Iterator {
        db: db,
        iter: db.bc.NewIterator(),
    }
}

func (it *Iterator) Close() {
    it.iter.Close()
}

func (it *Iterator) SeekTo(key []byte) []byte {
    return it.iter.Seek(key)
}

func (it *Iterator) SeekToFirst() {
    it.iter.SeekToFirst()
}

func (it *Iterator) SeekToLast() {
    it.iter.SeekToLast()
}

func (it *Iterator) Valid() bool {
    return it.err == nil && it.iter.Valid()
}

func (it *Iterator) Next() {
    it.iter.Next()
}

func (it *Iterator) Prev() {
    it.iter.Prev()
}

func (it *Iterator) Key() []byte {
    return it.iter.Key()
}

func (it *Iterator) Value() []byte {
    return it.iter.Value()
}

func (it *Iterator) Error() error {
    if it.err == nil {
        if err := it.iter.GetError(); err != nil {
            it.err = errors.Trace(err)
        }
    }
    return it.err
}

