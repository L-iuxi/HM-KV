package kv

import (
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutReply, error) {

	op := &proto.Op{
		Type:            "Put",
		Key:             req.Key,
		Value:           req.Value,
		ExpectedVersion: req.ExpectedVersion,
		ExpireAt:        req.ExpireAt,
		LeaseId:         req.LeaseId,
		ClientId:        req.ClientId,
		RequestId:       req.RequestId,
	}
	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.PutReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader}, nil
	}

	ch := make(chan result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return &proto.PutReply{
			Error:    res.Err,
			Version:  res.Version,
			LeaderId: leader,
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-time.After(5 * time.Second):
		return &proto.PutReply{
			Error: proto.ErrorType_TIMEOUT,
		}, nil
	}

}
