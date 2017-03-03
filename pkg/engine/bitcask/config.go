package bitcask

import "github.com/reborndb/go/bytesize"

type Config struct {
    MaxFileSize         int `toml:max_file_size`
    MaxOpenFiles        int `toml:max_open_file`
}

func NewDefaultConfig() *Config {
    return &Config {
        MaxFileSize:        bytesize.MB * 200,
        MaxOpenFiles:       4096,
    }
}

