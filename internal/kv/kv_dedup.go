package kv

import (
	"encoding/json"
	"fmt"
)

// dedupKey 查重表在 Badger 里的保留 key。
// 不含 "/"，mvcc.Recover（recover.go）扫描时按 "/" 切分，无 "/" 会直接跳过，不会污染 mvcc。
const dedupKey = "__dedup__"

// dedupPersist 查重表的持久化形态，与内存中的三个 map 一一对应。
type dedupPersist struct {
	LastRequest   map[int64]int64   `json:"last_request"`
	LastResult    map[int]result    `json:"last_result"`
	LastTxnResult map[int]txnresult `json:"last_txn_result"`
}

// persistDedup 把查重表整体写穿到 Badger。每次 apply 更新查重后调用。
// 调用方已持有 kv.mu（applyBatch 内），故这里不加锁。
func (kv *KvServer) persistDedup() {
	data, err := json.Marshal(dedupPersist{
		LastRequest:   kv.lastRequest,
		LastResult:    kv.lastResult,
		LastTxnResult: kv.lastTxnResult,
	})
	if err != nil {
		fmt.Printf("[dedup] marshal error: %v\n", err)
		return
	}
	if err := kv.store.RawPut(dedupKey, data); err != nil {
		fmt.Printf("[dedup] persist error: %v\n", err)
	}
}

// loadDedup 启动时从 Badger 读回查重表。无记录（首次启动）则保持空 map。
func (kv *KvServer) loadDedup() {
	data, err := kv.store.RawGet(dedupKey)
	if err != nil {
		// 无查重记录，首次启动，保持 InitKvserver 里建好的空 map
		return
	}

	var d dedupPersist
	if err := json.Unmarshal(data, &d); err != nil {
		fmt.Printf("[dedup] unmarshal error: %v\n", err)
		return
	}

	if d.LastRequest != nil {
		kv.lastRequest = d.LastRequest
	}
	if d.LastResult != nil {
		kv.lastResult = d.LastResult
	}
	if d.LastTxnResult != nil {
		kv.lastTxnResult = d.LastTxnResult
	}
}
