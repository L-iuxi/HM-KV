package kv

import (
	types "TicketX/internal/type"
	"TicketX/internal/watch"
	"TicketX/proto"
	"fmt"
	"time"
)

func (kv *KvServer) HandlePut(op *proto.Op) types.Result {
	// 去重
	last := kv.lastRequest[op.ClientId]
	if op.RequestId <= last {
		return kv.lastResult[int(op.ClientId)]
	}

	res := kv.putInternal(
		op.Key,
		op.Value,
		op.ExpectedVersion,
		op.LeaseId,
		op.ExpireAt,
	)

	// 记录请求结果
	kv.lastRequest[op.ClientId] = op.RequestId
	kv.lastResult[int(op.ClientId)] = res

	return res
}

func (kv *KvServer) putInternal(key string, value string, expectedVersion int64, leaseID int64, expireAt int64) result {

	rev, err := kv.mvcc.PutWithCAS(
		key,
		value,
		expectedVersion,
	)

	if err != nil {
		latestRev, _ := kv.mvcc.GetLatest(key)

		return result{
			Err:     proto.ErrorType_WRONG_VERSION,
			Version: latestRev,
		}
	}

	// Lease
	if leaseID != 0 {
		_ = kv.leaseMgr.Attach(key, leaseID)
	} else if expireAt != 0 {
		now := time.Now().Unix()
		id := kv.leaseMgr.Grant(expireAt, now)
		_ = kv.leaseMgr.Attach(key, id)
	}

	// Watch
	kv.watcherManager.Notify(watch.WatchEvent{
		Type:     "Put",
		Key:      key,
		Value:    value,
		Revision: rev,
	})

	return result{
		Err:     proto.ErrorType_OK,
		Version: rev,
	}
}
func (kv *KvServer) HandleGet(op *proto.Op) result {
	return kv.getInternal(
		op.Key,
		op.ExpectedVersion,
	)
}

func (kv *KvServer) getInternal(key string, expectedVersion int64) result {

	value, rev, err := kv.mvcc.Get(
		key,
		expectedVersion,
	)

	if err != nil {
		return result{
			Err: proto.ErrorType_KEY_NOT_EXIST,
		}
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

	res := kv.deleteInternal(
		op.Key,
		op.Value,
	)

	kv.lastRequest[op.ClientId] = op.RequestId
	kv.lastResult[int(op.ClientId)] = res

	return res
}
func (kv *KvServer) deleteInternal(key string, value string) result {

	rev := kv.mvcc.Delete(key)

	kv.watcherManager.Notify(watch.WatchEvent{
		Type:     "Delete",
		Key:      key,
		Value:    value,
		Revision: rev,
	})

	return result{
		Err:     proto.ErrorType_OK,
		Version: rev,
	}
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

// 处理事务
func (kv *KvServer) HandleTxn(op *proto.Op) txnresult {
	// 去重
	last := kv.lastRequest[op.ClientId]
	if op.RequestId <= last {
		return kv.lastTxnResult[int(op.ClientId)]
	}

	result := txnresult{
		Results: make([]*proto.KeyValue, 0),
		Err:     proto.ErrorType_OK,
	}

	success := kv.CompareAll(op.Compares)

	if success {
		result.Succeeded = true
		result.Results = kv.executeEntries(op.SuccessEntries)
	} else {
		result.Succeeded = false
		result.Results = kv.executeEntries(op.FailedEntries)
	}

	result.Version = kv.mvcc.CurrentRev()

	kv.lastRequest[op.ClientId] = op.RequestId
	kv.lastTxnResult[int(op.ClientId)] = result

	return result
}

// 比较条件
func (kv *KvServer) CompareAll(compares []*proto.Compare) bool {

	for _, compare := range compares {
		if !kv.Compare(compare) {
			return false
		}
	}
	return true
}

// 比较单个条件
func (kv *KvServer) Compare(c *proto.Compare) bool {
	revision, ok := kv.mvcc.GetLatest(c.Key)

	// key 不存在
	if !ok {
		revision = 0
	}

	switch c.CompareType {
	case proto.CompareType_EQUAL:
		return revision == c.Version

	case proto.CompareType_GREATER:
		return revision > c.Version

	case proto.CompareType_LESS:
		return revision < c.Version

	case proto.CompareType_NOT_EQUAL:
		return revision != c.Version

	default:
		return false
	}
}

// 处理日志
func (kv *KvServer) executeEntries(entries []*proto.Entry) []*proto.KeyValue {

	results := make([]*proto.KeyValue, 0)

	for _, entry := range entries {
		switch entry.Type {

		case "Get":

			res := kv.getInternal(entry.Key, 0)

			if res.Err == proto.ErrorType_OK {
				results = append(results, &proto.KeyValue{
					Key:     entry.Key,
					Value:   res.Value,
					Version: res.Version,
				})
			}

		case "Put":
			kv.putInternal(entry.Key, entry.Value, 0, entry.LeaseId, entry.ExpireAt)

		case "Delete":
			kv.deleteInternal(entry.Key, entry.Value)
		}
	}

	return results
}
