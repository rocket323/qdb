// +build all bitcask

package bitcask

import (
    "fmt"
    "os"
    "github.com/juju/errors"
    "github.com/reborndb/qdb/pkg/engine"
    bcask "github.com/rocket323/bitcask"
)

type BitCask struct {
    path    string
    bc      *bcask.BitCask
    opts    *bcask.Options
}

var (
    ErrNotSupported = fmt.Errorf("not supported")
)

func (db *BitCask) IsKeyOrdered() bool {
    return false
}

func Open(path string, conf *Config, repair bool) (*BitCask, error) {
    db := &BitCask{}
    if err := db.init(path, conf, repair); err != nil {
        db.Close()
        return nil, errors.Trace(err)
    }
    return db, nil
}

func (db *BitCask) init(path string, conf *Config, repair bool) error {
    if conf == nil {
        conf = NewDefaultConfig()
    }

    // Create path if not exists first
    if err := os.MkdirAll(path, 0700); err != nil {
        return errors.Trace(err)
    }

    opts := bcask.NewOptions()
    opts.SetMaxFileSize(int64(conf.MaxFileSize))
    opts.SetMaxOpenFiles(int32(conf.MaxOpenFiles))

    db.path = path
    db.opts = opts

    var err error
    if db.bc, err = bcask.Open(db.path, db.opts); err != nil {
        return errors.Trace(err)
    }
    return nil
}

func (db *BitCask) Clear() error {
    if db.bc != nil {
        db.bc.Close()
        db.bc = nil
        // destroy and reopen database
        if err := bcask.DestroyDatabase(db.path); err != nil {
            return errors.Trace(err)
        } else if db.bc, err = bcask.Open(db.path, db.opts); err != nil {
            return errors.Trace(err)
        }
    }
    return nil
}

func (db *BitCask) Close() {
    if db.bc != nil {
        db.bc.Close()
    }
}

func (db *BitCask) NewSnapshot() engine.Snapshot {
    return newSnapshot(db)
}

func (db *BitCask) Get(key []byte) ([]byte, error) {
    value, err := db.bc.Get(string(key))
    if err == bcask.ErrNotFound {
        err = nil
    }
    return value, errors.Trace(err)
}

func (db *BitCask) Commit(bt *engine.Batch) error {
    if bt.OpList.Len() == 0 {
        return nil
    }
    var err error
    for e := bt.OpList.Front(); e != nil; e = e.Next() {
        switch op := e.Value.(type) {
        case *engine.BatchOpSet:
            err = db.bc.Set(string(op.Key), op.Value)
            if err != nil {
                return err
            }
        case *engine.BatchOpDel:
            err = db.bc.Del(string(op.Key))
            if err != nil {
                return err
            }
        default:
            panic(fmt.Sprintf("unsupported batch operation: %+v", op))
        }
    }
    return nil
}

func (db *BitCask) NewIterator() engine.Iterator {
    panic("not supported")
    return nil
}

func (db *BitCask) Compact(start, limit []byte) error {
    return nil
}

func (db *BitCask) Stats() string {
    return "bitcask engine"
}

