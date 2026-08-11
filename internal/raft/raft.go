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

// electionTimeout 随机生成选举超时。
// 用 per-node random source（种子 = 启动时间纳秒 + me），确保多节点同时重启后
// 不同节点的超时不同，打破对称性避免 perpetual split vote。
func (rf *Raft) electionTimeout() time.Duration {
	min := rf.cfg.ElectionTimeoutMin
	max := rf.cfg.ElectionTimeoutMax
	jitter := time.Duration(rand.Int63n(int64(max-min)))
	// per-node 偏移：每个节点的选举超时基数不同
	offset := time.Duration(rf.me) * 150 * time.Millisecond
	return min + jitter + offset
}
// Stop 关闭 Raft 所有后台 goroutine（ticker、ApplyLoop）。
// 关闭后 Raft 节点不可再使用。
func (rf *Raft) Stop() {
	select {
	case <-rf.stopCh:
		// 已关闭，避免 panic
	default:
		close(rf.stopCh)
	}
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
		// 单节点：自己已构成多数派，无需等心跳响应
		if rf.readIndexCounter > len(rf.peers)/2 {
			close(rf.readIndexGate)
			rf.readIndexGate = nil
			rf.mu.Unlock()
			return readIndex, nil
		}
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
		// 检查是否被 Stop
		select {
		case <-rf.stopCh:
			return
		default:
		}

		rf.mu.Lock()
		states := rf.states
		rf.mu.Unlock()

		switch states {

		case Follower, Candidate: //follower和Candidate
			select {
			case <-rf.overElectiontime.C: //到达选举时间，给select一个信号使它触发
				rf.startElection()
			case <-rf.stopCh:
				return
			default:
				time.Sleep(10 * time.Millisecond)
			}
		case Leader: //leader发送心跳

			select {
			case <-rf.heartbeat.C:
				rf.broadcastAppendEntries() //心跳
				rf.heartbeat.Reset(rf.cfg.HeartbeatInterval)
			case <-rf.stopCh:
				return
			default:
				time.Sleep(10 * time.Millisecond)
			}

		}
	}
}

func MakeRaft(applyCh chan ApplyMsg, peers []string, me int32, cfg RaftConfig, tlsDialOpt grpc.DialOption) *Raft {
	rf := &Raft{cfg: cfg}
	// WAL 路径：优先用 cfg.DataDir，兼容旧代码（DataDir 为空时回退到旧路径）
	walDir := cfg.DataDir
	if walDir == "" {
		walDir = "../download/wal"
	}
	rf.wal = wal.NewWal(walDir)
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
	rf.stopCh = make(chan struct{})

	// 保存 TLS 拨号选项，供后续 addPeers / 成员变更时使用
	rf.tlsDialOpt = tlsDialOpt

	rf.matchIndex = make([]int32, len(peers))
	rf.overElectiontime = time.NewTimer(rf.electionTimeout())
	for _, addr := range peers {
		rf.addPeers(addr)
	}
	rf.heartbeat = time.NewTimer(cfg.HeartbeatInterval)

	rf.log = []types.LogEntry{{}} //dummy节点，log的index从1开始

	// 发送初始化完成信号给 KV 层，使其关闭 readyCh。
	// 即使没有待 Apply 的 entry，Get 也需要知道 Raft 已就绪。
	go func() {
		rf.applyCh <- ApplyMsg{Init: true}
	}()

	go rf.ApplyLoop() //循环发送要执行的日志给kvserver
	go rf.ticker()
	return rf

}

// addPeers 建立到 peer 的 gRPC 连接。
//
// 连接方式取决于 MakeRaft 传入的 tlsDialOpt：
//   - tlsDialOpt != nil → 使用 TLS 加密连接（节点间通信加密）
//   - tlsDialOpt == nil → 使用 insecure 模式（开发/测试用）
//
// 在 TLS 模式下，节点证书需要包含该节点在 peers 列表中的地址作为 SAN，
// 否则对端验证证书时会失败（"certificate is valid for X, not Y"）。
func (rf *Raft) addPeers(addr string) error {
	var conn *grpc.ClientConn
	var err error

	if rf.tlsDialOpt != nil {
		// TLS 模式：证书加密的节点间通信
		conn, err = grpc.NewClient(addr, rf.tlsDialOpt)
	} else {
		// Insecure 模式：明文通信（默认，开发环境）
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if err != nil {
		return err
	}
	rf.clientConns = append(rf.clientConns, conn)
	rf.clients = append(rf.clients, proto.NewRaftClient(conn))

	return nil
}

// RefreshConnections 关闭所有旧的 peer gRPC 连接并重新建立。
// 用于重启场景：peer 的 gRPC server 就绪后调用，避免旧连接因 backoff
// 陷入 TRANSIENT_FAILURE 导致 RPC 持续失败。
func (rf *Raft) RefreshConnections() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, conn := range rf.clientConns {
		conn.Close()
	}
	rf.clientConns = nil
	rf.clients = nil
	for _, addr := range rf.peers {
		rf.addPeers(addr)
	}
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
