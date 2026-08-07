package kv

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"context"

	po "google.golang.org/protobuf/proto"
)

// Compact 删除 ≤revision 的旧历史版本，通过 Raft 提交保证集群一致
func (kv *KvServer) Compact(ctx context.Context, req *proto.CompactRequest) (*proto.CompactReply, error) {
	op := &proto.Op{
		Type:            "Compact",
		ExpectedVersion: req.Revision, //compact 版本号
		ClientId:        req.ClientId,
		RequestId:       req.RequestId,
	}
	data, _ := po.Marshal(op)

	index, _, isLeader, leader := kv.rf.Start(data)
	if !isLeader {
		return &proto.CompactReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan types.Result, 1)
	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch
	return &proto.CompactReply{
		Error:    res.Err,
		LeaderId: res.Version, //复用 version 字段传 leaderId
	}, nil
}

// HandleCompact Raft apply 时执行 compact
func (kv *KvServer) HandleCompact(op *proto.Op) result {
	rev := op.ExpectedVersion //compact 版本号
	if err := kv.mvcc.Compact(rev); err != nil {
		return result{Err: proto.ErrorType_INTERNAL_ERROR}
	}
	kv.watcherManager.Compact(rev)
	return result{Err: proto.ErrorType_OK}
}
