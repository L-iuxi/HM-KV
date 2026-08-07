package kv

import (
	types "TicketX/internal/type"
	"TicketX/internal/raft"
	"TicketX/internal/watch"
	"TicketX/proto"
	"fmt"
	"time"

	po "google.golang.org/protobuf/proto"
)

// 记录批量提交
func (kv *KvServer) applier() {

	batch := make([]raft.ApplyMsg, 0)

	ticker := time.NewTicker(kv.cfg.KV.ApplyBatchInterval)

	for {

		select {

		case msg := <-kv.applyCh: //从管道取消息先放进batch缓冲区

			batch = append(batch, msg)

			if len(batch) >= 100 {
				kv.applyBatch(batch) //处理batch里面的消息
				batch = nil
			}

		case <-ticker.C:

			if len(batch) > 0 {
				kv.applyBatch(batch)
				batch = nil
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

func (kv *KvServer) HandlePut(op *proto.Op) types.Result {
	// 去重
	last := kv.lastRequest[op.ClientId]
	if op.RequestId <= last {
		return kv.lastResult[int(op.ClientId)]
	}

	rev, err := kv.mvcc.PutWithCAS(op.Key, op.Value, op.ExpectedVersion)
	if err != nil {
		latestRev, _ := kv.mvcc.GetLatest(op.Key)
		res := result{
			Err:     proto.ErrorType_WRONG_VERSION,
			Version: latestRev,
		}
		kv.lastResult[int(op.ClientId)] = res
		return res
	}

	kv.lastRequest[op.ClientId] = op.RequestId //记录该clientid最后一个请求结果

	// 如果指定了已有 lease，直接绑定（多 key 共享 lease）
	if op.LeaseId != 0 {
		_ = kv.leaseMgr.Attach(op.Key, op.LeaseId)
	} else if op.ExpireAt != 0 {
		// 带 TTL：创建新 lease 并绑定
		now := time.Now().Unix()
		leaseID := kv.leaseMgr.Grant(op.ExpireAt, now)
		_ = kv.leaseMgr.Attach(op.Key, leaseID)
	}

	fmt.Printf("[kv] Put key=%s rev=%d\n", op.Key, rev)
	kv.watcherManager.Notify(watch.WatchEvent{
		Type:     "Put",
		Key:      op.Key,
		Value:    op.Value,
		Revision: rev,
	})
	res := result{
		Err:     proto.ErrorType_OK,
		Version: rev,
	}
	kv.lastResult[int(op.ClientId)] = res

	return res

}

func (kv *KvServer) HandleGet(op *proto.Op) result {
	value, rev, err := kv.mvcc.Get(op.Key, op.ExpectedVersion)
	if err != nil {
		return result{Err: proto.ErrorType_KEY_NOT_EXIST}
	}
	return result{
		Value:   value,
		Version: rev,
		Err:     proto.ErrorType_OK,
	}
}

func (kv *KvServer) HandleDelete(op *proto.Op) result {
	// 去重
	last := kv.lastRequest[op.ClientId]
	if op.RequestId <= last {
		return kv.lastResult[int(op.ClientId)]
	}

	rev := kv.mvcc.Delete(op.Key)

	fmt.Printf("[kv] Delete key=%s rev=%d\n", op.Key, rev)
	kv.lastRequest[op.ClientId] = op.RequestId //记录该clientid最后一个请求结果

	kv.watcherManager.Notify(watch.WatchEvent{
		Type:     "Delete",
		Key:      op.Key,
		Value:    op.Value,
		Revision: rev,
	})

	res := result{
		Err:     proto.ErrorType_OK,
		Version: rev,
	}
	kv.lastResult[int(op.ClientId)] = res
	return res
}

func findLE(revs []int64, target int64) int64 {
	var res int64 = 0

	for _, r := range revs {
		if r <= target {
			res = r
		} else {
			break
		}
	}

	return res
}

// 客户端批量提交，多个 Entry 打包成一个 Raft entry，原子执行
func (kv *KvServer) HandleBatch(op *proto.Op) result {
	// 去重
	last := kv.lastRequest[op.ClientId]
	if op.RequestId <= last {
		return kv.lastResult[int(op.ClientId)]
	}

	for i, entry := range op.Entries {
		// 构造子 Op
		subOp := &proto.Op{
			Type:      entry.Type,
			Key:       entry.Key,
			Value:     entry.Value,
			ClientId:  op.ClientId,
			RequestId: op.RequestId + int64(i+1),
		}
		switch entry.Type {
		case "Put":
			kv.HandlePut(subOp)
		case "Delete":
			kv.HandleDelete(subOp)
		}
	}

	kv.lastRequest[op.ClientId] = op.RequestId

	res := result{
		Err:     proto.ErrorType_OK,
		Version: kv.mvcc.CurrentRev(),
	}
	kv.lastResult[int(op.ClientId)] = res
	return res
}

// 移除过期的建
func (kv *KvServer) expireByKey(key string) {
	fmt.Printf("[lease] expireByKey: key=%s\n", key)
	rev := kv.mvcc.Delete(key)
	id, err := kv.leaseMgr.GetLeaseIDByKey(key)
	if err == nil {
		_ = kv.leaseMgr.RemoveLease(id)
		fmt.Printf("[lease] expireByKey: removed lease %d for key %s\n", id, key)
	}
	kv.watcherManager.Notify(watch.WatchEvent{
		Type:     "Delete",
		Key:      key,
		Revision: rev,
	})
}
