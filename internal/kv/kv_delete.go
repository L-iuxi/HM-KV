package kv

import (
	"TicketX/proto"
	"context"

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

	ch := make(chan result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.DeleteReply{
		Error:    res.Err,
		LeaderId: leader,
		Version:  res.Version,
	}, nil
}
