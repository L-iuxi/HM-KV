package raft

import (
	"TicketX/proto"
	"context"
	"fmt"
)

// 发起选举
func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.states = Candidate
	rf.term++

	fmt.Printf("[raft] node %d: starting election, term %d\n", rf.me, rf.term)
	rf.vote = int32(rf.me)

	term := rf.term
	me := rf.me

	lastIndex := rf.getLastIndex()
	lastTerm := rf.log[lastIndex-rf.lastSnapIndex].Term

	if !rf.overElectiontime.Stop() {
		select {
		case <-rf.overElectiontime.C:
		default:
		}
	}

	// 重置选举计时器,避免重复选举
	rf.overElectiontime.Reset(rf.electionTimeout())

	// 拷贝 peers 和 clients，防止并发成员变更导致下标越界
	peers := make([]string, len(rf.peers))
	copy(peers, rf.peers)
	clients := make([]proto.RaftClient, len(rf.clients))
	copy(clients, rf.clients)

	rf.mu.Unlock()

	votes := 1

	for i, addr := range peers {
		if i == me {
			continue
		}

		go func(client proto.RaftClient, peerAddr string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  int32(me),
				LastLogIndex: lastIndex,
				LastLogTerm:  int32(lastTerm),
			}

			reply := &RequestVoteReply{}

			if !rf.sendRequestVoteTo(client, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 这次选举已经失效
			if rf.states != Candidate || rf.term != term {
				return
			}
			//发现更高term
			if reply.Term > rf.term {
				fmt.Printf("[raft] node %d: stepping down to Follower (higher term %d from vote reply), was term %d\n", rf.me, reply.Term, rf.term)
				rf.term = reply.Term
				rf.states = Follower
				rf.vote = -1
				return
			}

			if reply.IsVote == 1 {
				votes++
				if votes > len(rf.peers)/2 {
					rf.states = Leader
					fmt.Printf("[raft] node %d: became Leader, term %d\n", rf.me, rf.term)

					for j := range rf.peers {
						rf.nextIndex[j] = rf.getLastIndex() + 1
						rf.matchIndex[j] = 0
					}
					//立刻心跳
					//防止其他节点再次发起选举，通知所有选举自己是leader
					go rf.broadcastAppendEntries()
				}
			}
		}(clients[i], addr)
	}
}

// sendRequestVoteTo 向目标 follower 发送投票请求。client 由调用方在锁内取出，避免无锁访问 rf.clients。
func (rf *Raft) sendRequestVoteTo(client proto.RaftClient, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
	defer cancel()

	res, err := client.RequestVote(ctx, &proto.RequestVoteArgs{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateId),
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	})
	if err != nil {
		return false
	}

	reply.Term = int32(res.Term)
	reply.IsVote = 0
	if res.VoteGranted {
		reply.IsVote = 1
	}
	return true
}

// 向目标follower发送投票请求
func (rf *Raft) sendRequestVote(server int32, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
	defer cancel()

	res, err := rf.clients[server].RequestVote(ctx, &proto.RequestVoteArgs{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateId),
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	})
	//fmt.Printf("我收到了票 %d ", reply.IsVote)
	if err != nil {
		return false
	}

	reply.Term = int32(res.Term)
	reply.IsVote = 0
	if res.VoteGranted {
		reply.IsVote = 1
	}
	return true
}

/*
1.检查leader的最新日志任期以及最新日志位置是否比自己新
2.检查leader任期是否大于自己，否则不投票，如果大于，更新自己的任期和在当前任期的投票
3.版本号相同且未投票，此时可以投票
*/

// 接收投票请求，投出票
func (rf *Raft) RequestVote(ctx context.Context, args *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &proto.RequestVoteReply{}

	//fmt.Printf("我在给%d投票", args.CandidateId)
	reply.VoteGranted = false

	upToDate := false
	lastIndex := rf.getLastIndex()
	lastTerm := rf.log[lastIndex-rf.lastSnapIndex].Term

	//判断日志新不新
	if args.LastLogTerm > int32(lastTerm) || (args.LastLogTerm == int32(lastTerm) && args.LastLogIndex >= lastIndex) {
		upToDate = true
	}

	//版本号大了，更新版本号，清空投票
	if args.Term > rf.term {
		fmt.Printf("[raft] node %d: reverting to Follower (RequestVote from node %d, term %d > %d)\n", rf.me, args.CandidateId, args.Term, rf.term)
		rf.term = args.Term
		rf.vote = -1
		rf.states = Follower
		rf.persistVote(int64(args.Term), int64(args.CandidateId), reply.VoteGranted)
	}

	//版本号小了，不投票
	if args.Term < rf.term {
		reply.VoteGranted = false
		reply.Term = rf.term
		return reply, nil
	}

	if args.Term == rf.term &&
		(rf.vote == -1 || rf.vote == args.CandidateId) && upToDate { //版本号相同，未投票

		rf.vote = args.CandidateId
		reply.VoteGranted = true
		rf.persistVote(int64(args.Term), int64(args.CandidateId), reply.VoteGranted)

		rf.overElectiontime.Reset(rf.electionTimeout())
	}
	reply.Term = rf.term

	return reply, nil
}
