// +build all bitcask

package bitcask

import (
    "github.com/reborndb/qdb/pkg/engine"
    bcask "github.com/rocket323/bitcask"
)

type Snapshot struct {
    db *BitCask
    snap *bcask.Snapshot
}

func newSnapshot(db *BitCask) *Snapshot {
    snap := db.bc.NewSnapshot()
    return &Snapshot {
        db: db,
        snap: snap,
    }
}

func (sp *Snapshot) Close() {
    sp.db.bc.ReleaseSnapshot(sp.snap)
}

func (sp *Snapshot) NewIterator() engine.Iterator {
    return newIterator(sp.db, sp)
}

func (sp *Snapshot) Get(key []byte) ([]byte, error) {
    panic("not supported")
}

