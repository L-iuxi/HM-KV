package kv

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) Batch(ctx context.Context, req *proto.BatchRequest) (*proto.BatchReply, error) {
	// 把多个 Entry 打包成一个 Op，走一次 Raft
	op := &proto.Op{
		Type:      "Batch",
		Entries:   req.Entries,
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
	}
	data, _ := po.Marshal(op)

	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.BatchReply{
			Success:  false,
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
		return &proto.BatchReply{
			Success: res.Err == proto.ErrorType_OK,
			Error:   res.Err,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-time.After(5 * time.Second):
		return &proto.BatchReply{
			Error: proto.ErrorType_TIMEOUT,
		}, nil
	}

}
