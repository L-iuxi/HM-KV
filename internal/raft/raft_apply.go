package raft

import "time"

// 检查是否有新日志可以提交，更新已执行的日志
func (rf *Raft) ApplyLoop() {
	for {

		// 检查是否被 Stop
		select {
		case <-rf.stopCh:
			return
		default:
		}

		rf.mu.Lock()
		if rf.lastApply < rf.commitIndex { //提交从commitindex到lastapply这个区间的日志
			rf.lastApply++
			logIdx := rf.lastApply - rf.lastSnapIndex

			// 边界保护：快照截断后 log 变短，lastApply 可能已超出 log 范围。
			// 该 entry 已被快照覆盖，跳过即可（快照安装时已同步到 KV 层）。
			if logIdx < 0 || int(logIdx) >= len(rf.log) {
				rf.mu.Unlock()
				continue
			}

			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: int64(rf.lastApply),
				Command:      rf.log[logIdx].Command,
			}

			rf.mu.Unlock()
			select {
			case rf.applyCh <- msg: //将要执行的日志传给上层kvserver
			case <-rf.stopCh:
				return
			}
		} else { //没有新日志要提交
			rf.mu.Unlock()
			select {
			case <-time.After(10 * time.Millisecond):
			case <-rf.stopCh:
				return
			}
		}
	}
}
