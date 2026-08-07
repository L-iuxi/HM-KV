package raft

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// 发送快照
func (rf *Raft) sendInstallSnapshotTo(client proto.RaftClient, peer int, peerAddr string, pGen int64) {

	rf.mu.Lock()
	data := make([]byte, len(rf.snap))
	copy(data, rf.snap)

	args := &proto.InstallSnapshotArgs{
		Term:          int32(rf.term),
		LeaderId:      int32(rf.me),
		LastSnapIndex: int32(rf.lastSnapIndex),
		LastSnapTerm:  int32(rf.lastSnapTerm),
		Data:          data,
	}
	rf.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.ReadIndexTimeout)
	defer cancel()

	res, err := client.InstallSnapshot(ctx, args)
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// RPC 回来后校验成员可能已变更
	if rf.peerGen != pGen || peer >= len(rf.peers) || rf.peers[peer] != peerAddr {
		return
	}

	if int32(res.Term) > rf.term {
		rf.term = int32(res.Term)
		rf.states = Follower
		return
	}

	rf.nextIndex[peer] = rf.lastSnapIndex + 1
	rf.matchIndex[peer] = rf.lastSnapIndex
}

// 接受快照作为自己的日志
func (rf *Raft) InstallSnapshot(ctx context.Context, args *proto.InstallSnapshotArgs) (*proto.InstallSnapshotReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &proto.InstallSnapshotReply{}

	//确认自己收到的快照leader任期大于自己
	if args.Term < rf.term {
		reply.Term = rf.term
		return reply, nil
	}

	//更新任期
	rf.states = Follower
	rf.term = args.Term

	//判断快照是否比自己的快照位置靠后
	if args.LastSnapIndex <= rf.lastSnapIndex {
		return reply, nil
	}

	//保存快照数据，写入磁盘
	snapshotPath := filepath.Join(rf.wal.Dir(), fmt.Sprintf("snapshot-%d-%d.dat", args.LastSnapIndex, args.LastSnapTerm))
	if err := os.WriteFile(snapshotPath, args.Data, 0644); err != nil {
		fmt.Printf("Failed to write received snapshot to file: %v", err)
	}

	//快照记录写入WAL
	if err := rf.persistSnapshot(int64(args.LastSnapIndex), int64(args.LastSnapTerm), snapshotPath); err != nil {
		fmt.Printf("Failed to persist snapshot: %v\n", err)
	}

	// 删除不需要的与快照重复的旧日志文件
	if err := rf.wal.Truncate(uint64(args.LastSnapIndex)); err != nil {
		fmt.Printf("Failed to truncate WAL after snapshot install: %v", err)
	}

	oldSnapIndex := rf.lastSnapIndex

	//截断旧日志，在快照内的日志不需要了
	if args.LastSnapIndex < rf.getLastIndex() {
		rf.log = rf.log[args.LastSnapIndex-oldSnapIndex:]
	} else { //丢弃旧log
		rf.log = []types.LogEntry{{Term: args.LastSnapTerm}}
	}

	//更新
	rf.lastSnapIndex = args.LastSnapIndex
	rf.lastSnapTerm = args.LastSnapTerm

	rf.commitIndex = max(rf.commitIndex, rf.lastSnapIndex)
	rf.lastApply = max(rf.lastApply, rf.lastSnapIndex)

	rf.snap = make([]byte, len(args.Data))
	copy(rf.snap, args.Data)

	return reply, nil
}

// LogSize 返回当前内存日志的近似大小（字节），供 KV 层判断是否需要快照。
func (rf *Raft) LogSize() int64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	total := 0
	for _, e := range rf.log {
		total += len(e.Command)
	}
	return int64(total)
}

// 生成快照
func (rf *Raft) Snapshot(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//确定快照位置
	snapIndex := rf.getLastIndex()
	snapTerm := rf.log[snapIndex-rf.lastSnapIndex].Term

	//保存快照文件
	snapshotPath := filepath.Join(rf.wal.Dir(), fmt.Sprintf("snapshot-%d-%d.dat", snapIndex, snapTerm))
	if err := os.WriteFile(snapshotPath, data, 0644); err != nil {
		fmt.Printf("Snapshot: failed to write file: %v\n", err)
		return
	}

	//写WAl快照
	if err := rf.persistSnapshot(int64(snapIndex), int64(snapTerm), snapshotPath); err != nil {
		fmt.Printf("Snapshot: failed to persist record: %v\n", err)
		return
	}

	//删除旧日志
	if err := rf.wal.Truncate(uint64(snapIndex)); err != nil {
		fmt.Printf("Snapshot: failed to truncate WAL: %v\n", err)
		return
	}

	oldSnapIndex := rf.lastSnapIndex
	rf.log = rf.log[snapIndex-oldSnapIndex:]
	rf.log[0].Command = nil // dummy 占位

	rf.lastSnapIndex = snapIndex
	rf.lastSnapTerm = snapTerm

	rf.snap = make([]byte, len(data))
	copy(rf.snap, data)
}
