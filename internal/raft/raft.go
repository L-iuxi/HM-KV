package raft

import (
	types "TicketX/internal/type"
	"TicketX/internal/wal"
	"TicketX/proto"
	"fmt"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// electionTimeout 随机生成选举超时
func (rf *Raft) electionTimeout() time.Duration {
	min := rf.cfg.ElectionTimeoutMin
	max := rf.cfg.ElectionTimeoutMax
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.commitIndex)
}
func (rf *Raft) LastHeartbeat() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartbeat
}

// 获取当前节点在当前任期是否leader
func (rf *Raft) GetState() (int32, bool) {

	var term int32
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.states == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.term
	return term, isleader
}

// 未快照截断的日志长度
func (rf *Raft) getLastIndex() int32 {
	return int32(int(rf.lastSnapIndex) + len(rf.log) - 1)
}

// 确认自己能不能快速读
func (rf *Raft) ReadIndex() (int32, error) {
	rf.mu.Lock()
	if rf.states != Leader { //当前已不是leader
		rf.mu.Unlock()
		return 0, fmt.Errorf("not leader")
	}

	readIndex := rf.commitIndex
	rf.readIndexTerm = rf.term
	rf.readIndexGen++
	rf.readIndexCounter = 1 //自己
	rf.readIndexGate = make(chan struct{})
	rf.mu.Unlock()

	//发起一次心跳，确认自己还是不是leader
	rf.heartbeat.Reset(0)

	select {
	case <-rf.readIndexGate:
		return readIndex, nil
	case <-time.After(rf.cfg.ReadIndexTimeout):
		rf.mu.Lock()
		rf.readIndexGate = nil
		rf.mu.Unlock()
		return 0, fmt.Errorf("read index timeout")
	}
}

func (rf *Raft) Start(data []byte) (int32, int32, bool, int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.term
	index := rf.getLastIndex() + 1 //日志编号
	isleader := true

	if rf.states != Leader {
		isleader = false
		return -1, term, isleader, rf.nowLeader
	} //不是leader不复制

	newcomm := types.LogEntry{
		Index:   index,
		Term:    rf.term,
		Command: data,
	}
	rf.nowLeader = int64(rf.me)
	rf.log = append(rf.log, newcomm)

	//	rf.commitIndex++
	rf.persistEntry(newcomm)
	go rf.broadcastAppendEntries()

	return int32(index), term, isleader, rf.nowLeader
}

// 无限循环选举，心跳发送
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		states := rf.states
		rf.mu.Unlock()

		switch states {

		case Follower, Candidate: //follower和Candidate
			select {
			case <-rf.overElectiontime.C: //到达选举时间，给select一个信号使它触发
				rf.startElection()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		case Leader: //leader发送心跳

			select {
			case <-rf.heartbeat.C:
				rf.broadcastAppendEntries() //心跳
				rf.heartbeat.Reset(rf.cfg.HeartbeatInterval)
			default:
				time.Sleep(10 * time.Millisecond)
			}

		}
	}
}

func MakeRaft(applyCh chan ApplyMsg, peers []string, me int32, cfg RaftConfig) *Raft {
	rf := &Raft{cfg: cfg}
	rf.wal = wal.NewWal("../download/wal")
	if rf.wal.Exists() {
		rf.LoadFromWAL()
	}
	rf.me = int(me)
	rf.peers = peers
	rf.term = 0
	rf.states = Follower
	rf.vote = -1
	rf.lastSnapIndex = 0
	rf.lastSnapTerm = 0
	rf.nextIndex = make([]int32, len(peers))
	rf.commitIndex = 0   //刚开始没有待提交的日志
	rf.lastApply = 0     //刚开始没有已经执行的日志
	rf.applyCh = applyCh //与上层kvserver联系的管道

	rf.matchIndex = make([]int32, len(peers))
	rf.overElectiontime = time.NewTimer(rf.electionTimeout())
	for _, addr := range peers {
		rf.addPeers(addr)
	}
	rf.heartbeat = time.NewTimer(cfg.HeartbeatInterval)

	rf.log = []types.LogEntry{{}} //dummy节点，log的index从1开始

	go rf.ApplyLoop() //循环发送要执行的日志给kvserver
	go rf.ticker()
	return rf

}
func (rf *Raft) addPeers(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	rf.clientConns = append(rf.clientConns, conn)
	rf.clients = append(rf.clients, proto.NewRaftClient(conn))

	return nil
}

// findPeerIndex 按地址查找 peer 下标，-1 表示不存在。调用方必须持有 rf.mu。
func (rf *Raft) findPeerIndex(addr string) int {
	for i, a := range rf.peers {
		if a == addr {
			return i
		}
	}
	return -1
}
