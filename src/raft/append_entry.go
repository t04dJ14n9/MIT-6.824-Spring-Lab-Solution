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
				for peer := 0; peer < len(rf.peers); peer++ {
					if peer == rf.me {
						continue
					}
					rf.doAppendEntryForPeer(peer)
				}
			}

			// reset last check time disregarding role
			rf.appendEntryBaseline = time.Now()
		}
		rf.mu.Unlock()
		time.Sleep(CheckInterval)
	}
}

func (rf *Raft) doAppendEntryForPeer(peer int) {
	// copy log to send.
	// since the testing env is simulated on local machine, if logToSend is not copied, race condition will happen since no lock is held in rf.sendAppendEntries
	logToSend := rf.log[rf.nextIndex[peer]:]
	logToSendCopies := make([]LogEntry, len(logToSend))
	saveTerm := rf.currentTerm
	copy(logToSendCopies, logToSend)

	prevLogIndex := rf.nextIndex[peer] - 1
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      logToSendCopies,
		LeaderCommit: rf.commitIndex,
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
			if rf.nextIndex[peer] > 1 {
				rf.updateNextIndex(peer, &arg, reply.ConflictIndex, reply.ConflictTerm)
			}
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	sortedList := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			sortedList[i] = rf.getLastLogIndex()
		} else {
			sortedList[i] = rf.matchIndex[i]
		}
	}
	sort.Ints(sortedList)
	// only commit log entries that is committed in current term
	newCommitIndex := sortedList[len(rf.peers)/2]
	for newCommitIndex > rf.commitIndex {
		if rf.currentTerm == rf.log[newCommitIndex].Term {
			DPrintf("Peer[%d]: updating commitIndex: %d => %d", rf.me, rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
			return
		}
		newCommitIndex--
	}
}

func (rf *Raft) updateNextIndex(peer int, arg *AppendEntriesArgs, conflictIndex int, conflictTerm int) {
	// case 1, conflictTerm is larger or -1(None), impossible to get that term using backtracking
	if conflictTerm > rf.log[arg.PrevLogIndex].Term || conflictTerm <= 0 {
		rf.nextIndex[peer] = max(1, conflictIndex)
		return
	}
	// case 2, keep on backtracking until term is no larger than conflictTerm
	i := arg.PrevLogIndex
	for i > 0 && rf.log[i].Term > conflictTerm {
		i--
	}
	// if the entire conflictTerm does not exist in leader's log, skip over the entire term
	if rf.log[i].Term < conflictTerm {
		rf.nextIndex[peer] = max(1, conflictIndex)
		return
	}
	// leader has conflictTerm in its log, set nextIndex to the one beyond last index of that term in its log
	// i is the last index of conflictTerm
	rf.nextIndex[peer] = max(1, i+1)
	return
}
