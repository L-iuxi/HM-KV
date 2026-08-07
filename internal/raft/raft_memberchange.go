package raft

import (
	types "TicketX/internal/type"
	"TicketX/proto"
)

func (rf *Raft) HandleConfigChange(change *proto.Memberchange) types.Result {
	switch change.Type {
	case "add":
		rf.handleAdd(change)
	case "delete":
		rf.handleDelete(change)
	default:
		return types.Result{Err: proto.ErrorType_INVALID_ARGUMENT}
	}
	return types.Result{Err: proto.ErrorType_OK}
}

func (rf *Raft) handleAdd(change *proto.Memberchange) (types.Result, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//已经存在
	for _, peer := range rf.peers {
		if peer == change.Address {
			return types.Result{
				Err: proto.ErrorType_MEMBER_ALREADY_EXISTS,
			}, nil
		}
	}

	//添加单个节点
	if err := rf.addPeers(change.Address); err != nil {
		return types.Result{
			Err: proto.ErrorType_INTERNAL_ERROR,
		}, err
	}
	// 添加到raft集群
	rf.peers = append(rf.peers, change.Address)

	last := rf.getLastIndex()
	rf.nextIndex = append(rf.nextIndex, last+1)
	rf.matchIndex = append(rf.matchIndex, 0)
	rf.peerGen++

	return types.Result{
		Err: proto.ErrorType_OK,
	}, nil

}

// 删除某个节点
func (rf *Raft) handleDelete(change *proto.Memberchange) (types.Result, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	idx := -1
	oldAddr := rf.peers[rf.me]
	for i, peer := range rf.peers {

		if peer == change.Address {
			idx = i
			break
		}
	}

	//没找到
	if idx == -1 {
		return types.Result{
			Err: proto.ErrorType_MEMBER_NOT_FOUND,
		}, nil
	}

	// 关闭被删除节点的 gRPC 连接
	if idx < len(rf.clientConns) && rf.clientConns[idx] != nil {
		rf.clientConns[idx].Close()
	}

	rf.peers = append(rf.peers[:idx], rf.peers[idx+1:]...)
	rf.clientConns = append(rf.clientConns[:idx], rf.clientConns[idx+1:]...)
	rf.clients = append(rf.clients[:idx], rf.clients[idx+1:]...)
	rf.nextIndex = append(rf.nextIndex[:idx], rf.nextIndex[idx+1:]...)
	rf.matchIndex = append(rf.matchIndex[:idx], rf.matchIndex[idx+1:]...)

	// 删除后重新计算 rf.me（必须在 slice 操作之后）
	rf.me = -1
	for i, addr := range rf.peers {
		if addr == oldAddr {
			rf.me = i
			break
		}
	}
	rf.peerGen++

	return types.Result{
		Err: proto.ErrorType_OK,
	}, nil
}
