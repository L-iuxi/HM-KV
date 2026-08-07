package kv

import (
	"TicketX/proto"
	types "TicketX/internal/type"
	"context"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) AddMember(ctx context.Context, req *proto.AddMemberRequest) (*proto.AddMemberResponse, error) {
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
		return &proto.AddMemberResponse{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan types.Result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.AddMemberResponse{
		Error:    res.Err,
		LeaderId: leader,
	}, nil
}

func (kv *KvServer) deleteMember(ctx context.Context, req *proto.DeleteMemberRequest) (*proto.DeleteMemberResponse, error) {
	change := &proto.Memberchange{
		Type: "delete",
		Id:   req.Id,
	}

	op := &proto.Op{
		Type:         "CONFIG_CHANGE",
		MemberChange: change,
	}

	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.DeleteMemberResponse{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan types.Result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.DeleteMemberResponse{
		Error:    res.Err,
		LeaderId: leader,
	}, nil
}
