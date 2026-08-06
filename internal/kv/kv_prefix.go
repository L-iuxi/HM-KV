package kv

import (
	"TicketX/internal/mvcc"
	"TicketX/proto"
)

// 前缀扫描：直接用 BadgerDB 前缀扫描，按原始 key 分组取最新 revision
func (kv *KvServer) readByPrefix(prefix string) (*proto.GetReply, error) {
	kvs, err := kv.mvcc.PrefixScan(prefix)
	if err != nil {
		return &proto.GetReply{Error: proto.ErrorType_INTERNAL_ERROR}, nil
	}

	var result []*proto.KeyValue
	for _, entry := range kvs {
		result = append(result, &proto.KeyValue{
			Key:     entry.Key,
			Value:   entry.Value,
			Version: entry.Version,
		})
	}

	return &proto.GetReply{
		Error: proto.ErrorType_OK,
		Kvs:   result,
	}, nil
}

// 类型转换
func toProto(kvs []mvcc.KeyValue) []*proto.KeyValue {
	var result []*proto.KeyValue
	for _, kv := range kvs {
		result = append(result, &proto.KeyValue{Key: kv.Key, Value: kv.Value, Version: kv.Version})
	}
	return result
}
