// +build all bitcask

package bitcask

import (
    "fmt"
    "github.com/reborndb/qdb/pkg/engine"
)

type driver struct {
}

func (d driver) Open(path string, conf interface{}, repair bool) (engine.Database, error) {
    cfg, ok := conf.(*Config)
    if !ok {
        return nil, fmt.Errorf("conf type is not bitcask config, invalid")
    }
    return Open(path, cfg, repair)
}

func init() {
    engine.Register("bitcask", driver{})
}

