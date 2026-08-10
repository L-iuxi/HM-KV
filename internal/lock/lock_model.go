package lock

import (
	"TicketX/internal/lease"
	"TicketX/internal/mvcc"
	types "TicketX/internal/type"
	"TicketX/internal/watch"
	"TicketX/proto"
)

type LockManager struct {
	leaseMgr *lease.LeaseManager
	mvcc     *mvcc.MVCC
	watcher  *watch.WatcherManager
	executor TxnExecutor
}

type TxnExecutor interface {
	ExecuteTxn(op *proto.Op) (types.TxnResult, error)
}
