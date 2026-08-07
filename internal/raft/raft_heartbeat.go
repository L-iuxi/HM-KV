package raft

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"context"
	"math/rand"
	"time"
)

// 向所有follwer发送心跳
func (rf *Raft) broadcastAppendEntries() {
	//先确认自己状态，同时拷贝 peer 列表防止并发成员变更
	rf.mu.Lock()
	if rf.states != Leader {
		rf.mu.Unlock()
		return
	}

	readGen := rf.readIndexGen
	pGen := rf.peerGen
	me := rf.me
	peers := make([]string, len(rf.peers))
	copy(peers, rf.peers)
	rf.mu.Unlock()

	//遍历本地副本，每个follower一个goroutine
	for i, addr := range peers {
		if i == me {
			continue
		}

		go func(peerAddr string, peerIdx int, pGen int64, readGen int) {
			rf.mu.Lock()
			// O(1) 校验：成员变更后 gen 不一致 / peer 已删除 / 地址被替换
			if rf.peerGen != pGen || peerIdx >= len(rf.peers) || rf.peers[peerIdx] != peerAddr {
				rf.mu.Unlock()
				return
			}

			//确定leader对于这个follower要同步的位置
			next := rf.nextIndex[peerIdx]
			prevIndex := next - 1

			var prevTerm int32

			//要同步的位置已经变成冷冰冰的快照...
			//发一个快照过去
			if next <= rf.lastSnapIndex {
				client := rf.clients[peerIdx]
				rf.mu.Unlock()
				rf.sendInstallSnapshotTo(client, peerIdx, peerAddr, pGen)
				return //发快照就不发心跳
			}

			//记录要同步位置的任期
			if prevIndex >= 0 {
				prevTerm = int32(rf.log[prevIndex-rf.lastSnapIndex].Term)
			}

			//复制一份日志准备发送
			entries := make([]types.LogEntry, len(rf.log[next-rf.lastSnapIndex:]))
			copy(entries, rf.log[next-rf.lastSnapIndex:])

			args := &HeartbeatArgs{
				LeaderId:          int32(rf.me),
				LeaderTerm:        rf.term,
				Entries:           entries,
				PreLogIndex:       prevIndex,
				PreLogTerm:        prevTerm,
				LeaderCommitIndex: rf.commitIndex,
			}
			// 锁内取出 client，避免无锁访问 rf.clients
			client := rf.clients[peerIdx]
			rf.mu.Unlock()

			//发送
			reply := &HeartbeatReply{}
			ok := rf.sendAppendEntriesTo(client, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// RPC 回来后重新校验，成员可能已变更
			if rf.peerGen != pGen || peerIdx >= len(rf.peers) || rf.peers[peerIdx] != peerAddr {
				return
			}

			//任期落后，打为follower
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.states = Follower
				rf.vote = -1
				return
			}
			//如果在等待rpc返回的过程中当前不再是leader，返回
			if rf.states != Leader || args.LeaderTerm != rf.term {
				return
			}

			//同步成功，记录
			if reply.Success {
				rf.lastHeartbeat = time.Now()

				// ReadIndexcount受到超过半数就关闭
				if rf.readIndexGate != nil && rf.readIndexTerm == rf.term && rf.readIndexGen == readGen {
					rf.readIndexCounter++
					if rf.readIndexCounter > len(rf.peers)/2 {
						close(rf.readIndexGate)
						rf.readIndexGate = nil
					}
				}

				//记录下一次要同步的日志位置
				if len(args.Entries) > 0 {

					rf.nextIndex[peerIdx] = int32(int(args.PreLogIndex) + len(args.Entries) + 1)
					//成功对齐日志，记录成功数
					rf.matchIndex[peerIdx] = int32(int(args.PreLogIndex) + len(args.Entries))
				} else {
					rf.nextIndex[peerIdx] = args.PreLogIndex + 1
				}
				for N := rf.getLastIndex(); N > rf.commitIndex; N-- {
					if N <= rf.lastSnapIndex {
						continue
					}
					count := 1
					for j := range rf.peers {
						if j != rf.me && rf.matchIndex[j] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.log[N-rf.lastSnapIndex].Term == int32(rf.term) {
						rf.commitIndex = N
						break
					}
				}
			} else {
				//同步失败，记录下一次同步位置为冲突位置
				rf.nextIndex[peerIdx] = reply.ConflictIndex
				if rf.nextIndex[peerIdx] > rf.getLastIndex()+1 {
					rf.nextIndex[peerIdx] = rf.getLastIndex() + 1
				}

			}

		}(addr, i, pGen, readGen)
	}
}

/*
1.检查leader任期，大于等于自己认为依然可以作为leader，否则不承认
2.若大于，更新自己的任期，重置当前任期投票
3.刷新选举超时
4.检查上一次的同步日志位置，比较当前follwer和leader当前位置的数据，若相同可以保证之前内容相同，
若不同，向前找到最后同步位置，返回给leader
5.开始追加日志，检查要追加日志的位置是否有冲突，若有，删除冲突后追加，若无，追加日志

*/
//follower检查发来的日志并且检查，更新日志
func (rf *Raft) AppendEntries(ctx context.Context, args *proto.HeartbeatArgs) (*proto.HeartbeatReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &proto.HeartbeatReply{}
	if args.LeaderTerm < rf.term { //leader任期落后
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictIndex = int32(len(rf.log))
		return reply, nil
	}

	rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	if args.LeaderTerm > rf.term { //自己落后，更新任期
		rf.term = args.LeaderTerm
		rf.vote = -1
	}
	rf.states = Follower
	rf.nowLeader = int64(args.LeaderId)

	//准备同步的日志位置超过当前follower拥有的日志位置
	if args.PreLogIndex > rf.getLastIndex() {
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictIndex = rf.getLastIndex()
		return reply, nil
	}

	//leader发来的日志位置已经变成冷冰冰的快照了...
	if args.PreLogIndex < rf.lastSnapIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastSnapIndex + 1
		reply.Term = rf.term
		return reply, nil
	}

	//日志产生冲突，找到第一个冲突任期位置
	if args.PreLogIndex >= 0 && rf.log[args.PreLogIndex-rf.lastSnapIndex].Term != int32(args.PreLogTerm) {
		reply.Term = rf.term
		reply.Success = false
		index := args.PreLogIndex

		conflictTerm := rf.log[args.PreLogIndex-rf.lastSnapIndex].Term
		for index >= 0 && rf.log[index-rf.lastSnapIndex].Term == conflictTerm {
			index--
		}
		reply.ConflictIndex = index + 1
		return reply, nil
	}

	//leader的最后提交位置大于自己的最后提交位置，更新已被提交的日志位置
	if args.LeaderCommitIndex > rf.commitIndex { //leader比自己先提交
		rf.commitIndex = min(args.LeaderCommitIndex, rf.getLastIndex())
	}

	//追加日志
	index := args.PreLogIndex - rf.lastSnapIndex + 1
	incoming := make([]types.LogEntry, len(args.Entries))

	for i, e := range args.Entries {
		incoming[i] = types.LogEntry{
			Term:    int32(e.Term),
			Command: e.Command,
		}
	}

	for i, entry := range incoming {
		if int(index)+i < len(rf.log) {
			if rf.log[int(index)+i].Term != int32(entry.Term) { //发生冲突

				rf.log = rf.log[:int(index)+i] //从当前开始覆盖后面所有
				rf.log = append(rf.log, incoming[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, incoming[i:]...)
			break
		}
	}

	if len(args.Entries) > 0 {
		newLen := int(args.PreLogIndex) + len(args.Entries) + 1
		if newLen < len(rf.log) {
			rf.log = rf.log[:newLen]
		}
	}

	reply.Success = true
	reply.Term = rf.term
	return reply, nil
}

// sendAppendEntriesTo 向指定 follower 发送日志。client 由调用方在锁内取出，避免无锁访问 rf.clients。
func (rf *Raft) sendAppendEntriesTo(client proto.RaftClient, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
	defer cancel()

	entries := make([]*proto.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &proto.LogEntry{
			Term:    int64(e.Term),
			Command: e.Command,
		}
	}

	res, err := client.AppendEntries(ctx, &proto.HeartbeatArgs{
		LeaderId:          int32(args.LeaderId),
		LeaderTerm:        int32(args.LeaderTerm),
		PreLogIndex:       int32(args.PreLogIndex),
		PreLogTerm:        int32(args.PreLogTerm),
		Entries:           entries,
		LeaderCommitIndex: int32(args.LeaderCommitIndex),
	})

	if err != nil {
		return false
	}

	reply.Success = res.Success
	reply.Term = int32(res.Term)
	reply.ConflictIndex = int32(res.ConflictIndex)

	return true
}

// leader主动向某个follower发送日志
func (rf *Raft) SendAppendEntries(server int32, args *HeartbeatArgs, reply *HeartbeatReply) bool {

	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
	defer cancel()

	entries := make([]*proto.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &proto.LogEntry{
			Term:    int64(e.Term),
			Command: e.Command,
		}
	}

	res, err := rf.clients[server].AppendEntries(ctx, &proto.HeartbeatArgs{
		LeaderId:          int32(args.LeaderId),
		LeaderTerm:        int32(args.LeaderTerm),
		PreLogIndex:       int32(args.PreLogIndex),
		PreLogTerm:        int32(args.PreLogTerm),
		Entries:           entries,
		LeaderCommitIndex: int32(args.LeaderCommitIndex),
	})

	if err != nil {
		return false
	}

	reply.Success = res.Success
	reply.Term = int32(res.Term)
	reply.ConflictIndex = int32(res.ConflictIndex)

	return true
}
