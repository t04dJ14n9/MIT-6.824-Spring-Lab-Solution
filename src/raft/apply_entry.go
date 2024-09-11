package raft

import "time"

func (rf *Raft) applyEntryRoutine() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.lastApplied].Command,
			}
			DPrintf("Peer[%d]: applyMsg sent %+v", rf.me, applyMsg)
			rf.applyChan <- applyMsg
		}
		rf.mu.Unlock()
		time.Sleep(CheckInterval)
	}
}
