package kv

import (
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) Txn(ctx context.Context, req *proto.TxnRequest) (*proto.TxnReply, error) {

	op := &proto.Op{
		Type:           "Txn",
		Compares:       req.Compare,
		SuccessEntries: req.Success,
		FailedEntries:  req.Failed,
		ClientId:       req.ClientId,
		RequestId:      req.RequestId,
	}

	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.TxnReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader}, nil
	}

	ch := make(chan TxnResult, 1)

	kv.mu.Lock()
	kv.txnWaitCh[int64(index)] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		return &proto.TxnReply{
			Error:     res.Err,
			Results:   res.Results,
			LeaderId:  leader,
			Succeeded: res.Succeeded,
		}, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-time.After(5 * time.Second):
		return &proto.TxnReply{
			Error: proto.ErrorType_TIMEOUT,
		}, nil
	}
}
