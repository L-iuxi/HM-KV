package db

import (
	types "TicketX/internal/type"

	"github.com/dgraph-io/badger/v4"
)

type Store struct {
	DB *badger.DB
}

type KeyValue struct {
	Key   string
	Value string
}

// 前缀扫描结果，包含完整 BadgerDB key 和 Value
type ScanResult struct {
	Key     string // MVCC 格式的完整 key，如 "svc/user/5"
	Value   string
	Version int64
	Deleted bool
}

type KVStore interface {
	Put(key string, value types.Value) error //put操作

	Get(key string) (types.Value, error) //get操作

	Delete(key string) error //delete操作，标记deleted为true

	PrefixScan(prefix string) ([]ScanResult, error) //按前缀扫描

	ScanAll() ([]ScanResult, error) //从数据库恢复

	BatchDelete(keys []string) error //批量删除

	DropAll() error //清空所有数据（快照恢复用）

	Close() error //关闭
}
