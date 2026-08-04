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
