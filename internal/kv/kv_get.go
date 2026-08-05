package kv

import (
	"TicketX/proto"
	"context"
	"time"

	po "google.golang.org/protobuf/proto"
)

func (kv *KvServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetReply, error) {

	if req.Prefix { //按前缀读取
		return kv.readByPrefix(req.Key)
	}

	// 客观是否被认为是leader
	if readIndex, err := kv.rf.ReadIndex(); err == nil {

		for {
			kv.mu.Lock()
			la := kv.lastApplied
			kv.mu.Unlock()
			if la >= int64(readIndex) {
				break
			}
			time.Sleep(time.Millisecond)
		}

		value, rev, err := kv.mvcc.Get(req.Key, req.Version)
		if err == nil {
			return &proto.GetReply{
				Error:   proto.ErrorType_OK,
				Kvs:     []*proto.KeyValue{{Key: req.Key, Value: value}},
				Version: rev,
			}, nil
		}
		return &proto.GetReply{Error: proto.ErrorType_KEY_NOT_EXIST}, nil
	}

	//走raft读
	op := &proto.Op{
		Type:            "Get",
		Key:             req.Key,
		ExpectedVersion: req.Version,
		ClientId:        req.ClientId,
		RequestId:       req.RequestId,
	}
	data, _ := po.Marshal(op)

	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.GetReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan result, 1)
	kv.mu.Lock()
	kv.getCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	var kvs []*proto.KeyValue
	if res.Err == proto.ErrorType_OK {
		kvs = []*proto.KeyValue{{Key: op.Key, Value: res.Value}}
	}
	return &proto.GetReply{
		Error:   res.Err,
		Kvs:     kvs,
		Version: res.Version,
	}, nil
}
