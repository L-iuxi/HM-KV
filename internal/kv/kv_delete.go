package kv

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteReply, error) {
	op := &proto.Op{
		Type:      "Delete",
		Key:       req.Key,
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
	}
	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.DeleteReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader}, nil
	}

	ch := make(chan types.Result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:

		return &proto.DeleteReply{
			Error:   res.Err,
			Version: res.Version,
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-time.After(5 * time.Second):
		return &proto.DeleteReply{
			Error: proto.ErrorType_TIMEOUT,
		}, nil
	}
}
