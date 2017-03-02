// +build all bitcask

package bitcask

import (
    "bytes"
    "fmt"
)

type BitCask struct {
    path    string
    bc      *bcask.BitCask
    opts    *bcask.Options
}

func Open(path string, conf *Config, repair bool) (*BitCask, error) {
    db := &BitCask{}

}

