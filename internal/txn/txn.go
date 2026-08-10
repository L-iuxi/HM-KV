package txn

import "TicketX/proto"

// 服务器用事务实现分布式锁，其他事务由客户端封装好
func New() *Txn {
	return &Txn{}
}

func (t *Txn) BuildOp(clientID, requestID int64) *proto.Op {
	return &proto.Op{
		Type:           "Txn",
		Compares:       t.compares,
		SuccessEntries: t.successEntries,
		FailedEntries:  t.failedEntries,
		ClientId:       clientID,
		RequestId:      requestID,
	}
}

// 记录一个条件
func (t *Txn) If(compares ...*proto.Compare) *Txn {
	t.compares = append(t.compares, compares...)
	return t
}

// 描述条件成功之后的操作
func (t *Txn) Then(entries ...*proto.Entry) *Txn {
	t.successEntries = append(t.successEntries, entries...)
	return t
}

// 描述条件失败之后的操作
func (t *Txn) Else(entries ...*proto.Entry) *Txn {
	t.failedEntries = append(t.failedEntries, entries...)
	return t
}
