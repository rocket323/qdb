// +build all bitcask

package bitcask

import (
    "bytes"
    "fmt"
    bcask "github.com/rocket323/bitcask"
)

type BitCask struct {
    path    string
    bc      *bcask.BitCask
    opts    *bcask.Options
}

func Open(path string, conf *Config, repair bool) (*BitCask, error) {
    db := &BitCask{}
    if err := db.init(path, config, repair); err != nil {
        db.Close()
        return nil, errors.Trace(err)
    }
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
    opts.SetMaxFileSize(conf.MaxFileSize)
    opts.SetMaxOpenFiles(conf.MaxOpenFiles)

    db.path = path
    db.opts = opts

    var err error
    if db.bc, err = bcask.Open(db.path, db.opts); err != nil {
        return erros.Trace(err)
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
        } else db.bc, err = bcask.Open(db.path, db.opts); err != nil {
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

func (db *BitCask) NewIterator() engine.Iterator {
    return newIterator(db, nil)
}

func (db *BitCask) NewSnapshot() engine.Snapshot {
    return newSnapshot(db)
}

func (db *BitCask) Get(key []byte) ([]byte, error) {
    value, err := db.bc.Get(key)
    return value, errors.Trace(err)
}

func (db *BitCask) Commit(bt *engine.Batch) error {
    if bt.OpList.Len() == 0 {
        return nil
    }
    wb := bcask.NewWriteBatch()
    defer wb.Close()
    for e := bt.OpList.Front(); e != nil; e = e.Next() {
        switch op := e.Value().(type) {
        case *engine.BatchOpSet:
            wb.Put(op.Key, op.Value)
        case *engine.BatchOpDel:
            wb.Delete(op.Key)
        default:
            panic(fmt.Sprintf("unsupported batch operation: %+v", op))
        }
    }
    return errors.Trace(db.bc.Write(wb))
}

func (db *BitCask) Compact(start, limit []byte) error {
    return nil
}

func (db *BitCask) Stats() string {
    return ""
}

