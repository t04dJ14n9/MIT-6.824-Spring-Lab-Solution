package raft

import "time"

func (rf *Raft) applyEntryRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		applyQueue := make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.LogicIndex2RealIndex(rf.lastApplied)].Command,
			}
			applyQueue = append(applyQueue, applyMsg)
		}
		rf.mu.Unlock()

		for _, applyMsg := range applyQueue {
			DPrintf("Peer[%d]: applyMsg sent %+v", rf.me, applyMsg)
			rf.applyChan <- applyMsg
		}

		time.Sleep(CheckInterval)
	}
}
