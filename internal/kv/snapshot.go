package kv

import (
	"fmt"
	"time"
)

// snapshotWorker 定时检查 Raft 日志大小，超阈值时创建快照。
func (kv *KvServer) snapshotWorker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		if kv.rf.LogSize() < kv.cfg.KV.SnapshotThreshold {
			continue
		}

		data, err := kv.mvcc.Serialize()
		if err != nil {
			fmt.Printf("snapshot serialize error: %v\n", err)
			continue
		}

		kv.rf.Snapshot(data)
		fmt.Printf("snapshot created: %d bytes\n", len(data))
	}
}
