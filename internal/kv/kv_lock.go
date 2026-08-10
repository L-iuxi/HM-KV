package kv

import (
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

// Lock 分布式锁：通过 Txn CAS 创建唯一 key，若成功则等待前驱释放
func (kv *KvServer) Lock(ctx context.Context, req *proto.LockRequest) (*proto.LockReply, error) {
	// 校验 lease
	if _, err := kv.leaseMgr.GetLease(req.LeaseId); err != nil {
		return &proto.LockReply{
			Error:   proto.ErrorType_LEASE_NO_EXIST,
			Success: false,
		}, nil
	}

	// 构造 Txn：key 不存在则创建，绑定 lease
	lockKey := kv.loc.GenerateLockKey(req.Key, req.ClientId, req.RequestId)
	t := kv.loc.CreateLockTxn(lockKey, req.LeaseId)
	op := t.BuildOp(req.ClientId, req.RequestId)

	result, leader, err := kv.executeTxn(ctx, op)
	if err != nil {
		return &proto.LockReply{
			Error:     proto.ErrorType_INTERNAL_ERROR,
			Success:   false,
			LeaderId:  leader,
		}, nil
	}
	if result.Err != proto.ErrorType_OK {
		return &proto.LockReply{
			Error:     result.Err,
			Success:   false,
			LeaderId:  leader,
		}, nil
	}
	if !result.Succeeded {
		return &proto.LockReply{
			Error:     proto.ErrorType_LOCK_EXIST,
			Success:   false,
			LeaderId:  leader,
		}, nil
	}

	// TxnResult.Version = mvcc.CurrentRev() 即本 key 的 revision
	myRevision := result.Version

	// 等待前驱释放
	if err := kv.loc.WaitUntilAcquire(ctx, req.Key, myRevision); err != nil {
		return &proto.LockReply{
			Error:     proto.ErrorType_INTERNAL_ERROR,
			Success:   false,
			LeaderId:  leader,
		}, nil
	}

	return &proto.LockReply{
		Success:  true,
		Revision: myRevision,
		LeaderId: leader,
		Error:    proto.ErrorType_OK,
	}, nil
}

// Unlock 释放锁：CAS 删除 lock key
func (kv *KvServer) Unlock(ctx context.Context, req *proto.UnlockRequest) (*proto.UnlockReply, error) {
	// 构造 Txn：version 匹配则删除
	op := &proto.Op{
		Type: "Txn",
		Compares: []*proto.Compare{
			{
				Key:         req.LockKey,
				CompareType: proto.CompareType_EQUAL,
				Version:     req.Version,
			},
		},
		SuccessEntries: []*proto.Entry{
			{Type: "Delete", Key: req.LockKey},
		},
		FailedEntries: nil,
		ClientId:      req.ClientId,
		RequestId:     req.RequestId,
	}

	result, leader, err := kv.executeTxn(ctx, op)
	if err != nil {
		return &proto.UnlockReply{
			Success:   false,
			Error:     proto.ErrorType_INTERNAL_ERROR,
			LeaderId:  leader,
		}, nil
	}
	if result.Err != proto.ErrorType_OK {
		return &proto.UnlockReply{
			Success:   false,
			Error:     result.Err,
			LeaderId:  leader,
		}, nil
	}

	return &proto.UnlockReply{
		Success:   result.Succeeded,
		Error:     proto.ErrorType_OK,
		LeaderId:  leader,
	}, nil
}

// executeTxn 提交 Txn Op 到 Raft 并等待应用结果
func (kv *KvServer) executeTxn(ctx context.Context, op *proto.Op) (txnresult, int64, error) {
	data, _ := po.Marshal(op)
	index, _, isLeader, leader := kv.rf.Start(data)
	if !isLeader {
		return txnresult{Err: proto.ErrorType_WRONG_LEADER}, leader, nil
	}

	ch := make(chan txnresult, 1)

	kv.mu.Lock()
	kv.txnWaitCh[int64(index)] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return res, leader, nil

	case <-ctx.Done():
		return txnresult{}, leader, ctx.Err()

	case <-time.After(5 * time.Second):
		return txnresult{Err: proto.ErrorType_TIMEOUT}, leader, nil
	}
}
