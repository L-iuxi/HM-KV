package raft

import (
	"TicketX/internal/labgob"
	types "TicketX/internal/type"
	"TicketX/internal/wal"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// 从WAL恢复投票状态
func (rf *Raft) handleVoteRecord(data []byte) error {
	var voteRecord VoteRecord
	if err := labgob.NewDecoder(bytes.NewReader(data)).Decode(&voteRecord); err != nil {
		return fmt.Errorf("failed to decode vote record: %v", err)
	}

	rf.term = int32(voteRecord.Term)
	rf.vote = int32(voteRecord.CandidateID)
	return nil

}

// 从WAL恢复日志
func (rf *Raft) appendLogEntry(data []byte) error {
	var newLog types.LogEntry
	if err := labgob.NewDecoder(bytes.NewReader(data)).Decode(&newLog); err != nil {
		return fmt.Errorf("failed to decode vote record: %v", err)
	}

	newEntry := types.LogEntry{
		Index:   newLog.Index,
		Term:    newLog.Term,
		Command: newLog.Command,
	}

	rf.log = append(rf.log, newEntry)
	return nil
}

// 从wal恢复快照
func (rf *Raft) handleSnapshot(data []byte) error {
	var snapshot SnapshotRecord
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("Failed to unmarshal snapshot record: %v", err)
	}
	// When we find a snapshot, it becomes the new baseline.
	rf.lastSnapIndex = snapshot.LastIncludedIndex
	rf.lastSnapTerm = snapshot.LastIncludedTerm
	// The log entries before the snapshot are now irrelevant.
	rf.log = []types.LogEntry{{}}

	// Apply the snapshot to the state machine
	snapshotData, err := os.ReadFile(snapshot.Path)
	if err != nil {
		return fmt.Errorf("Failed to read snapshot file %s: %v", snapshot.Path, err)
	}
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshotData,
		SnapshotIndex: rf.lastSnapIndex + 1,
		SnapshotTerm:  rf.lastSnapTerm,
	}
	rf.applyCh <- applyMsg
	rf.lastApply = rf.lastSnapIndex
	rf.commitIndex = rf.lastSnapIndex
	return nil
}

// 从WAl恢复
func (rf *Raft) LoadFromWAL() error {
	records, types, err := rf.wal.LoadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %v", err)
	}

	for i, record := range records {
		switch types[i] {
		case wal.RecTypeEntry:
			rf.appendLogEntry(record) //恢复日志
		case wal.RecTypeState:
			rf.handleVoteRecord(record)
		case wal.RecTypeSnapshot:
			rf.handleSnapshot(record)
		}

	}
	return nil
}

// 把日志写入WAl
func (rf *Raft) persistEntry(entry types.LogEntry) {
	var buf bytes.Buffer
	if err := labgob.NewEncoder(&buf).Encode(entry); err != nil {
		fmt.Printf("Failed to encode log entry: %v\n", err)
	}
	if err := rf.wal.Write(wal.RecTypeEntry, buf.Bytes()); err != nil {
		fmt.Printf("Failed to write entry to WAL: %v\n", err)
	}

}

// 把投票记录写进WAL
func (rf *Raft) persistVote(term int64, candidateId int64, voteGranted bool) error {
	// 创建投票记录
	voteRecord := VoteRecord{
		Term:        term,
		CandidateID: candidateId,
		VoteGranted: voteGranted,
	}

	var buf bytes.Buffer
	if err := labgob.NewEncoder(&buf).Encode(voteRecord); err != nil {
		return fmt.Errorf("failed to encode vote record: %v", err)
	}

	if err := rf.wal.Write(wal.RecTypeState, buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write vote record to WAL: %v", err)
	}

	return nil
}

// 把快照记录写入WAL
func (rf *Raft) persistSnapshot(index int64, term int64, path string) error {

	snapshotRecord := SnapshotRecord{
		LastIncludedIndex: int32(index),
		LastIncludedTerm:  int32(term),
		Path:              path,
	}

	var buf bytes.Buffer

	if err := labgob.NewEncoder(&buf).Encode(snapshotRecord); err != nil {
		return fmt.Errorf("failed to encode snapshot record: %v", err)
	}

	if err := rf.wal.Write(wal.RecTypeSnapshot, buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write snapshot record to WAL: %v", err)
	}

	return nil
}
