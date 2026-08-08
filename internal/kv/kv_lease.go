package kv

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) KeepAlive(ctx context.Context, req *proto.KeepAliveRequest) (*proto.KeepAliveReply, error) {
	op := &proto.Op{
		Type: "KeepAlive",
		Key:  req.Key,
	}
	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.KeepAliveReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan types.Result, 1)
	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return &proto.KeepAliveReply{
			Error:    res.Err,
			LeaderId: leader,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-time.After(5 * time.Second):
		return &proto.KeepAliveReply{
			Error: proto.ErrorType_TIMEOUT,
		}, nil
	}

}

// HandleKeepAlive 续约：更新 key 对应 lease 的过期时间
func (kv *KvServer) HandleKeepAlive(op *proto.Op) result {
	err := kv.leaseMgr.KeepAliveByKey(op.Key, time.Now().Unix())
	if err != nil {
		return result{Err: proto.ErrorType_KEY_NOT_EXIST}
	}
	return result{Err: proto.ErrorType_OK}
}

// Grant 创建新 lease，返回 lease ID
func (kv *KvServer) Grant(ctx context.Context, req *proto.GrantRequest) (*proto.GrantReply, error) {
	op := &proto.Op{
		Type:     "Grant",
		ExpireAt: req.Ttl,
	}
	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.GrantReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan result, 1)
	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:

		return &proto.GrantReply{
			Error:   res.Err,
			LeaseId: res.LeaseID,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-time.After(5 * time.Second):
		return &proto.GrantReply{
			Error: proto.ErrorType_TIMEOUT,
		}, nil
	}

}

// HandleGrant 创建 lease（Raft apply）
func (kv *KvServer) HandleGrant(op *proto.Op) result {
	now := time.Now().Unix()
	leaseID := kv.leaseMgr.Grant(op.ExpireAt, now)
	return result{Err: proto.ErrorType_OK, LeaseID: leaseID}
}
