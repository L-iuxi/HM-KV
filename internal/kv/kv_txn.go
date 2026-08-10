package kv

import (
	types "TicketX/internal/type"
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

	result, err := kv.ExecuteTxn(op)

	if err != nil {
		return nil, err
	}

	return &proto.TxnReply{
		Error:     result.Err,
		Results:   result.Results,
		Succeeded: result.Succeeded,
	}, nil
}

func (kv *KvServer) ExecuteTxn(op *proto.Op) (types.TxnResult, error) {

	data, err := po.Marshal(op)
	if err != nil {
		return types.TxnResult{}, err
	}

	index, _, isLeader, _ := kv.rf.Start(data)

	if !isLeader {
		return types.TxnResult{
			Err: proto.ErrorType_WRONG_LEADER,
		}, nil
	}

	ch := make(chan types.TxnResult, 1)

	kv.mu.Lock()
	kv.txnWaitCh[int64(index)] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		return result, nil

	case <-time.After(5 * time.Second):
		return types.TxnResult{
			Err: proto.ErrorType_TIMEOUT,
		}, nil
	}
}
