package kv

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) AddMember(ctx context.Context, req *proto.AddMemberRequest) (*proto.AddMemberReply, error) {
	change := &proto.Memberchange{
		Type:    "add",
		Id:      req.Id,
		Address: req.Address,
	}

	op := &proto.Op{
		Type:         "CONFIG_CHANGE",
		MemberChange: change,
	}

	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.AddMemberReply{
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
		return &proto.AddMemberReply{
			Error:    res.Err,
			LeaderId: leader,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-time.After(5 * time.Second):
		return &proto.AddMemberReply{
			Error: proto.ErrorType_TIMEOUT,
		}, nil
	}

}

func (kv *KvServer) DeleteMember(ctx context.Context, req *proto.DeleteMemberRequest) (*proto.DeleteMemberReply, error) {
	change := &proto.Memberchange{
		Type:    "delete",
		Id:      req.Id,
		Address: req.Address,
	}

	op := &proto.Op{
		Type:         "CONFIG_CHANGE",
		MemberChange: change,
	}

	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.DeleteMemberReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan types.Result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.DeleteMemberReply{
		Error:    res.Err,
		LeaderId: leader,
	}, nil
}
