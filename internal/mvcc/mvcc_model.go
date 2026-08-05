package mvcc

import (
	"TicketX/internal/db"
	"sync"
	"time"
)

type MVCC struct {
	mu         sync.Mutex
	store      *db.Store
	currentRev int64              //全局版本
	latest     map[string]int64   //每个建的最新版本
	history    map[string][]int64 //每个建的历史版本
	revisions  []RevisionEntry    //每个版本都修改了什么建
	compactrev int64              //删除位置
	stop       chan struct{}      //停止compact
}

// 建和它被修改的版本
type RevisionEntry struct {
	Key      string
	Revision int64
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

	// 快照
	Serialize() ([]byte, error)
	Deserialize(data []byte) error

	// 后台 compact
	StartCompact(interval time.Duration)
}
