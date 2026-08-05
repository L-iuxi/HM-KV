package raft

import (
	types "TicketX/internal/type"
	"TicketX/internal/wal"
	"TicketX/proto"
	"sync"
	"time"
)

// 传输消息的结构体
type ApplyMsg struct {
	CommandIndex int64
	Command      interface{} //不关心类型
	CommandValid bool

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int32
	SnapshotIndex int32
}

// 定义server状态
type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

// 定义选票
type VoteRecord struct {
	Term        int64
	CandidateID int64
	VoteGranted bool
}

// RaftConfig 协议配置
type RaftConfig struct {
	ElectionTimeoutMin time.Duration // 选举超时下限
	ElectionTimeoutMax time.Duration // 选举超时上限
	HeartbeatInterval  time.Duration // Leader 心跳间隔
	RPCTimeout         time.Duration // Raft RPC 超时
	ReadIndexTimeout   time.Duration // ReadIndex 确认超时
}

// Raft结构体
type Raft struct {
	mu            sync.Mutex
	me            int      //当前服务器在peer的下标
	peers         []string //存有所有服务器的组
	clients       []proto.RaftClient
	states        State            //状态
	term          int32            //当前任期号
	vote          int32            //投票给
	log           []types.LogEntry //日志
	nowLeader     int64
	lastHeartbeat time.Time

	commitIndex      int32         //等待提交的最新日志编号
	nextIndex        []int32       //日志同步的位置（从哪里开始同步日志
	lastApply        int32         //上次执行的最后一条日志编号
	applyCh          chan ApplyMsg //与kvserver沟通的渠道
	heartbeat        *time.Timer   //心跳超时
	overElectiontime *time.Timer   //选举超时
	lastSnapIndex    int32         //上次截断日志的位置
	lastSnapTerm     int32         //上次截断日志的任期
	matchIndex       []int32
	snap             []byte
	wal              *wal.Wal
	cfg              RaftConfig //配置参数

	proto.UnimplementedRaftServer

	readIndexGate    chan struct{} //等待确认的通道
	readIndexTerm    int32         //发起readindex请求时候的任期
	readIndexCounter int           //受到的支持数
	readIndexGen     int           //第几次readindex
}

// 请求投票的结构体
type RequestVoteArgs struct {
	Term         int32 //当前任期号
	CandidateId  int32 //候选人id
	LastLogIndex int32 //候选人最新日志的index
	LastLogTerm  int32 //候选人最新日志的任期
}

// 投票给出的回复
type RequestVoteReply struct {
	Term   int32 //当前任期号
	IsVote int32 //是否投票
}

type HeartbeatArgs struct {
	LeaderId          int32
	LeaderTerm        int32
	Entries           []types.LogEntry
	PreLogIndex       int32 //最后对齐位置
	PreLogTerm        int32 //最后对齐位置的任期
	LeaderCommitIndex int32
}

type HeartbeatReply struct {
	Success       bool
	Term          int32
	ConflictIndex int32 //冲突位置
}

type SnapshotRecord struct {
	LastIncludedIndex int32
	LastIncludedTerm  int32
	Path              string
}

// RaftEngine KV 层调用的 Raft 接口
type RaftEngine interface {
	Start(data []byte) (index int32, term int32, isLeader bool, leaderId int64)
	GetState() (term int32, isLeader bool)
	ReadIndex() (int32, error)
	LogSize() int64
	Snapshot(data []byte)
}
