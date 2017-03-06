// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"github.com/reborndb/go/redis/rdb"
	redis "github.com/reborndb/go/redis/resp"
)

// DEL key [key ...]
func DelCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().Del(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// DUMP key
func DumpCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().Dump(s.DB(), args); err != nil {
		return toRespError(err)
	} else if dump, err := rdb.EncodeDump(x); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(dump), nil
	}
}

// TYPE key
func TypeCmd(s Session, args [][]byte) (redis.Resp, error) {
	if c, err := s.Store().Type(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString(c.String()), nil
	}
}

// EXISTS key
func ExistsCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().Exists(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// TTL key
func TTLCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().TTL(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PTTL key
func PTTLCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().PTTL(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PERSIST key
func PersistCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().Persist(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// EXPIRE key seconds
func ExpireCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().Expire(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PEXPIRE key milliseconds
func PExpireCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().PExpire(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// EXPIREAT key timestamp
func ExpireAtCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().ExpireAt(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// PEXPIREAT key timestamp
func PExpireAtCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().PExpireAt(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// RESTORE key ttlms value
func RestoreCmd(s Session, args [][]byte) (redis.Resp, error) {
	if err := s.Store().Restore(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

func init() {
	Register("del", DelCmd, CmdWrite | CmdNoOrder)
	Register("dump", DumpCmd, CmdReadonly)
	Register("exists", ExistsCmd, CmdReadonly)
	Register("expire", ExpireCmd, CmdWrite)
	Register("expireat", ExpireAtCmd, CmdWrite | CmdNoOrder)
	Register("persist", PersistCmd, CmdWrite)
	Register("pexpire", PExpireCmd, CmdWrite)
	Register("pexpireat", PExpireAtCmd, CmdWrite | CmdNoOrder)
	Register("pttl", PTTLCmd, CmdReadonly)
	Register("restore", RestoreCmd, CmdWrite)
	Register("ttl", TTLCmd, CmdReadonly)
	Register("type", TypeCmd, CmdReadonly)
}
