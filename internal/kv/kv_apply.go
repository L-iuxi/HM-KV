package kv

import (
	"TicketX/internal/raft"
	"TicketX/proto"
	"fmt"
	"time"

	po "google.golang.org/protobuf/proto"
)

// 记录批量提交
func (kv *KvServer) applier() {

	batch := make([]raft.ApplyMsg, 0)

	ticker := time.NewTicker(kv.cfg.KV.ApplyBatchInterval)
	recovered := false

	for {

		select {

		case msg := <-kv.applyCh: //从管道取消息先放进batch缓冲区

			batch = append(batch, msg)

			if len(batch) >= 100 {
				kv.applyBatch(batch) //处理batch里面的消息
				batch = nil
				if !recovered {
					recovered = true
					close(kv.readyCh)
				}
			}

		case <-ticker.C:

			if len(batch) > 0 {
				kv.applyBatch(batch)
				batch = nil
				if !recovered {
					recovered = true
					close(kv.readyCh)
				}
			} else if !recovered {
				// 没有积压的 WAL 条目，直接标记就绪
				recovered = true
				close(kv.readyCh)
			}
		}
	}
}

// 接受管道来到msg并执行
func (kv *KvServer) applyBatch(msg []raft.ApplyMsg) {

	for _, m := range msg {
		kv.lastApplied = m.CommandIndex
		if m.CommandValid {
			data := m.Command.([]byte)
			var op proto.Op
			err := po.Unmarshal(data, &op)
			if err != nil {
				fmt.Println("unmarshal error:", err)
				return
			}

			kv.mu.Lock()

			switch op.Type {

			case "Put":
				result := kv.HandlePut(&op)
				if ch, ok := kv.waitCh[m.CommandIndex]; ok {
					ch <- result
				}
				delete(kv.waitCh, m.CommandIndex)

			case "Grant":
				res := kv.HandleGrant(&op)
				if ch, ok := kv.waitCh[m.CommandIndex]; ok {
					ch <- res
				}
				delete(kv.waitCh, m.CommandIndex)
			case "Get":
				if ch, ok := kv.getCh[m.CommandIndex]; ok {
					result := kv.HandleGet(&op)
					ch <- result
				}

				delete(kv.getCh, m.CommandIndex)
			case "Delete":
				res := kv.HandleDelete(&op)
				if ch, ok := kv.waitCh[m.CommandIndex]; ok {
					ch <- res
					delete(kv.waitCh, m.CommandIndex)
				}
			case "Batch":
				res := kv.HandleBatch(&op)
				if ch, ok := kv.waitCh[m.CommandIndex]; ok {
					ch <- res
					delete(kv.waitCh, m.CommandIndex)
				}
			case "KeepAlive":
				res := kv.HandleKeepAlive(&op)
				if ch, ok := kv.waitCh[m.CommandIndex]; ok {
					ch <- res
				}
				delete(kv.waitCh, m.CommandIndex)
			case "Expire":
				kv.expireByKey(op.Key)
			case "Compact":
				res := kv.HandleCompact(&op)
				if ch, ok := kv.waitCh[m.CommandIndex]; ok {
					ch <- res
					delete(kv.waitCh, m.CommandIndex)
				}
			case "CONFIG_CHANGE":

				res := kv.rf.HandleConfigChange(op.MemberChange)
				if ch, ok := kv.waitCh[m.CommandIndex]; ok {
					ch <- res
					delete(kv.waitCh, m.CommandIndex)
				}
			case "Txn":
				res := kv.HandleTxn(&op)
				if ch, ok := kv.txnWaitCh[m.CommandIndex]; ok {
					ch <- res
					delete(kv.txnWaitCh, m.CommandIndex)
				}
			}

			kv.mu.Unlock()
		} else if m.SnapshotValid {
			kv.mu.Lock()
			if err := kv.mvcc.Deserialize(m.Snapshot); err != nil {
				fmt.Printf("snapshot recovery error: %v\n", err)
			}
			kv.lastApplied = int64(m.SnapshotIndex)
			kv.mu.Unlock()
		}

	}
}
