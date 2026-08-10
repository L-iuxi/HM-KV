package types

import "TicketX/proto"

type LogEntry struct {
	Index    int32
	Term     int32
	Version  int64
	Command  []byte
	ExpireAt int64
}
type Value struct {
	Value    string
	Version  int64
	ExpireAt int64
	Deleted  bool
}

// 把applyloop结果返回给put/get的
type Result struct {
	Value   string
	Version int64
	LeaseID int64
	Err     proto.ErrorType
}

// Txn事务的结果
type TxnResult struct {
	Err       proto.ErrorType
	Succeeded bool
	Version   int64
	Results   []*proto.KeyValue
}
