package raft

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Peer[%d]: CondInstallSnapshot(lastIncludedTerm=%v, lastIncludedIndex=%v)", rf.me, lastIncludedTerm, lastIncludedIndex)

	if rf.commitIndex >= lastIncludedIndex {
		DPrintf("Peer[%d]: rf.commitIndex >= snapshot.lastIncludedIndex, reject", rf.me)
		return false
	}
	if rf.getLogicLastLogIndex() > lastIncludedIndex {
		rf.log = append([]LogEntry{}, rf.log[rf.LogicIndex2RealIndex(lastIncludedIndex)+1:]...)
	} else {
		rf.log = []LogEntry{}
	}

	rf.persist()
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex

	rf.snapshot = snapshot
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	DPrintf("Peer[%d]: Snapshot success. rf.log=%v, rf.lastIncludedIndex=%v, rf.lastIncludedTerm=%v", rf.me, rf.log, rf.lastIncludedIndex, rf.lastIncludedTerm)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Peer[%d]: do Snapshot. index=%v, realIndex=%v, len(rf.log)=%v", rf.me, index, rf.LogicIndex2RealIndex(index), len(rf.log))
	if index > rf.getLogicLastLogIndex() {
		DPrintf("Peer[%d]: Snapshot() with index=%v > logicLastIndex=%v", rf.me, index, rf.getLogicLastLogIndex())
		return
	}
	rf.lastIncludedTerm = rf.log[rf.LogicIndex2RealIndex(index)].Term
	rf.log = append([]LogEntry{}, rf.log[rf.LogicIndex2RealIndex(index)+1:]...)
	rf.lastIncludedIndex = index
	rf.persist()
	rf.snapshot = snapshot
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	DPrintf("Peer[%d]: Snapshot success. rf.log=%v, rf.lastIncludedIndex=%v, rf.lastIncludedTerm=%v", rf.me, rf.log, rf.lastIncludedIndex, rf.lastIncludedTerm)
	return
}
