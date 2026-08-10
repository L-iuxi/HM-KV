package txn

import "TicketX/proto"

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
func (t *Txn) If(compares ...*proto.Compare) *Txn {
	t.compares = append(t.compares, compares...)
	return t
}

func (t *Txn) Then(entries ...*proto.Entry) *Txn {
	t.successEntries = append(t.successEntries, entries...)
	return t
}

func (t *Txn) Else(entries ...*proto.Entry) *Txn {
	t.failedEntries = append(t.failedEntries, entries...)
	return t
}
