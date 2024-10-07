package raft

import (
	"sort"
	"time"
)

func (rf *Raft) appendEntryRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		// if time passed since last check time > duration, check if it is leader
		if time.Since(rf.appendEntryBaseline) > rf.appendEntryDuration {
			// if is leader, start append entry for each peer
			if rf.role == leader {
				DPrintf("Peer[%d]: sending Appentry for each peer. rf.NextIndex=%+v, rf.lastIncludedIndex=%v", rf.me, rf.nextIndex, rf.lastIncludedIndex)
				for peer := 0; peer < len(rf.peers); peer++ {
					if peer == rf.me {
						continue
					}
					if rf.nextIndex[peer] <= rf.lastIncludedIndex {
						rf.doInstallSnapshotForPeer(peer)
					} else {
						rf.doAppendEntryForPeer(peer)
					}
				}
			}
			// reset last check time disregarding role
			rf.appendEntryBaseline = time.Now()
		}
		rf.mu.Unlock()
		time.Sleep(CheckInterval)
	}
}

func (rf *Raft) doInstallSnapshotForPeer(peer int) {
	saveTerm := rf.currentTerm
	arg := InstallSnapshotArgs{
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.snapshot,
		LeaderID:          rf.me,
		Term:              rf.currentTerm,
	}

	go func() {
		var reply InstallSnapshotReply
		ok := rf.sendInstallSnapshot(peer, &arg, &reply)
		if !ok {
			DPrintf("Peer[%d] => Peer[%d]: InstallSnapshot failed", rf.me, peer)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("Peer[%d] => Peer[%d]: AppendEntry response received %+v", rf.me, peer, reply)
		if reply.Term > rf.currentTerm {
			DPrintf("Peer[%d] => Peer[%d]: Received AppendEntries reply with higher term: %d, currentTerm: %d. Convert to follower",
				rf.me, peer, reply.Term, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.role = follower
			rf.votedFor = -1
			rf.persist()
			return
		}

		if rf.currentTerm != saveTerm || rf.role != leader {
			DPrintf("Peer[%d] => Peer[%d]: term or role changed during appendEntry, abort", rf.me, peer)
			return
		}

		rf.nextIndex[peer] = max(rf.nextIndex[peer], arg.LastIncludedIndex+1)
		rf.matchIndex[peer] = max(rf.nextIndex[peer], arg.LastIncludedIndex)
		rf.updateCommitIndex()
	}()
}

func (rf *Raft) doAppendEntryForPeer(peer int) {
	// copy log to send.
	// since the testing env is simulated on local machine, if logToSend is not copied, race condition will happen since no lock is held in rf.sendAppendEntries
	logToSend := rf.log[rf.LogicIndex2RealIndex(rf.nextIndex[peer]):]
	logToSendCopies := make([]LogEntry, len(logToSend))
	saveTerm := rf.currentTerm
	copy(logToSendCopies, logToSend)

	prevLogIndex := rf.nextIndex[peer] - 1
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		Entries:      logToSendCopies,
		LeaderCommit: rf.commitIndex,
	}
	if rf.LogicIndex2RealIndex(prevLogIndex) >= 0 {
		arg.PrevLogTerm = rf.log[rf.LogicIndex2RealIndex(prevLogIndex)].Term
	} else if prevLogIndex == rf.lastIncludedIndex {
		arg.PrevLogTerm = rf.lastIncludedTerm
	} else {
		panic("PrevLogIndex is inside the snapshot, should not happen")
	}

	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, &arg, &reply)
		if !ok {
			DPrintf("Peer[%d] => Peer[%d]: AppendEntry failed", rf.me, peer)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("Peer[%d] => Peer[%d]: AppendEntry response received %+v", rf.me, peer, reply)
		if reply.Term > rf.currentTerm {
			DPrintf("Peer[%d] => Peer[%d]: Received AppendEntries reply with higher term: %d, currentTerm: %d. Convert to follower",
				rf.me, peer, reply.Term, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.role = follower
			rf.votedFor = -1
			rf.persist()
			return
		}

		if rf.currentTerm != saveTerm || rf.role != leader {
			DPrintf("Peer[%d] => Peer[%d]: term or role changed during appendEntry, abort", rf.me, peer)
			return
		}

		if reply.Success {
			// maybe there has been a long delay and another round of append entry has increased the nextIndex and matchIndex
			rf.nextIndex[peer] = max(rf.nextIndex[peer], arg.PrevLogIndex+len(arg.Entries)+1)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], arg.PrevLogIndex+len(arg.Entries))
			rf.updateCommitIndex()
		} else {
			rf.updateNextIndex(peer, &arg, reply.ConflictIndex, reply.ConflictTerm)
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	sortedList := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			sortedList[i] = rf.getLastLogicalLogIndex()
		} else {
			sortedList[i] = rf.matchIndex[i]
		}
	}
	sort.Ints(sortedList)
	// only commit log entries that is committed in current term
	newCommitIndex := sortedList[len(rf.peers)/2]
	for newCommitIndex > rf.commitIndex && rf.LogicIndex2RealIndex(newCommitIndex) >= 0 {
		if rf.currentTerm == rf.log[rf.LogicIndex2RealIndex(newCommitIndex)].Term {
			DPrintf("Peer[%d]: updating commitIndex: %d => %d", rf.me, rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
			return
		}
		newCommitIndex--
	}
}

func (rf *Raft) updateNextIndex(peer int, arg *AppendEntriesArgs, conflictIndex int, conflictTerm int) {
	DPrintf("Updating Peer[%d].nextIndex[%d]=%v, conflictIndex=%v, conflictTerm=%v", rf.me, peer, rf.nextIndex[peer], conflictIndex, conflictTerm)
	// case 1, conflictTerm is larger or -1(None), impossible to get that term using backtracking
	if rf.LogicIndex2RealIndex(arg.PrevLogIndex) < 0 || rf.LogicIndex2RealIndex(arg.PrevLogIndex) >= len(rf.log) ||
		conflictTerm > rf.log[rf.LogicIndex2RealIndex(arg.PrevLogIndex)].Term || conflictTerm <= 0 {
		DPrintf("Peer[%d].nextIndex[%d]: %d => %d", rf.me, peer, rf.nextIndex[peer], conflictIndex)
		rf.nextIndex[peer] = conflictIndex
		return
	}
	// case 2, keep on backtracking until term is no larger than conflictTerm
	i := arg.PrevLogIndex
	for rf.LogicIndex2RealIndex(i) >= 0 && rf.log[rf.LogicIndex2RealIndex(i)].Term > conflictTerm {
		i--
	}
	// if the entire conflictTerm does not exist in leader's log, skip over the entire term
	if rf.LogicIndex2RealIndex(i) < 0 || rf.log[rf.LogicIndex2RealIndex(i)].Term < conflictTerm {
		DPrintf("Peer[%d].nextIndex[%d]: %d => %d", rf.me, peer, rf.nextIndex[peer], conflictIndex)
		rf.nextIndex[peer] = conflictIndex
		return
	}
	// leader has conflictTerm in its log, set nextIndex to the one beyond last index of that term in its log
	// i is the last index of conflictTerm
	DPrintf("Peer[%d].nextIndex[%d]: %d => %d", rf.me, peer, rf.nextIndex[peer], i+1)
	rf.nextIndex[peer] = i + 1
	return
}
