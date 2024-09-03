package raft

import "time"

func (rf *Raft) applyEntryRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			// TODO: apply log entry to local state machine
		}
		time.Sleep(CheckInterval)
	}
}
