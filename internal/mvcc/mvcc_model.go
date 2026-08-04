package mvcc

import (
	"TicketX/internal/db"
	"sync"
)

type MVCC struct {
	mu         sync.Mutex
	store      *db.Store
	currentRev int64              //全局版本
	latest     map[string]int64   //每个建的最新版本
	history    map[string][]int64 //每个建的历史版本
	compactrev int64              //删除位置
}

// KeyValue 前缀扫描结果
type KeyValue struct {
	Key   string
	Value string
}

type Engine interface {
	Put(key, value string) int64
	Get(key string, version int64) (string, int64, error)
	Delete(key string) int64
	PrefixScan(prefix string) ([]KeyValue, error)

	PutWithCAS(key, value string, expectedVersion int64) (int64, error)

	GetLatest(key string) (int64, bool)
	GetHistory(key string) []int64
	CurrentRev() int64
}
