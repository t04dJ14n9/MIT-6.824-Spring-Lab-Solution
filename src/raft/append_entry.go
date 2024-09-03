package raft

import "time"

func (rf *Raft) appendEntryRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == leader && time.Since(rf.appendEntryBaseline) > rf.appendEntryDuration {
			rf.doAppendEntry()
			goto SLEEP
		}
		rf.mu.Unlock()
	SLEEP:
		time.Sleep(CheckInterval)
	}
}

func (rf *Raft) doAppendEntry() {
	rf.appendEntryBaseline = time.Now()

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		arg := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.getLastLogIndex(),
			PrevLogTerm:  0,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}
		var reply AppendEntriesReply
		go rf.sendAppendEntries(peer, &arg, &reply)
	}
	rf.mu.Unlock()
}
