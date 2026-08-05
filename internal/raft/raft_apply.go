package raft

import "time"

// 检查是否有新日志可以提交，更新已执行的日志
func (rf *Raft) ApplyLoop() {
	for {

		rf.mu.Lock()
		if rf.lastApply < rf.commitIndex { //提交从commitindex到lastapply这个区间的日志
			rf.lastApply++
			//fmt.Println("我要提交日志")
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: int64(rf.lastApply),
				Command:      rf.log[rf.lastApply-rf.lastSnapIndex].Command,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg //将要执行的日志传给上层kvserver
		} else { //没有新日志要提交
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}
