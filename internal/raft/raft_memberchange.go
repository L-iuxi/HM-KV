package raft

import (
	"TicketX/proto"
	types "TicketX/internal/type"
)

func (raft *Raft) HandleConfigChange(op *proto.Op) types.Result {
	return types.Result{Err: proto.ErrorType_OK}
}
