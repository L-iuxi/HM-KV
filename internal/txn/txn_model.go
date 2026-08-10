package txn

import "TicketX/proto"

type Txn struct {
	compares       []*proto.Compare
	successEntries []*proto.Entry
	failedEntries  []*proto.Entry
}
