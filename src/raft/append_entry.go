package raft

import (
	"sort"
	"time"
)

func (rf *Raft) appendEntryRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == leader && time.Since(rf.appendEntryBaseline) > rf.appendEntryDuration {
			rf.appendEntryBaseline = time.Now()
			for peer := 0; peer < len(rf.peers); peer++ {
				if peer == rf.me {
					continue
				}
				rf.doAppendEntryForPeer(peer)
			}
			rf.mu.Unlock()
			goto SLEEP
		}
		rf.mu.Unlock()
	SLEEP:
		time.Sleep(CheckInterval)
	}
}

func (rf *Raft) doAppendEntryForPeer(peer int) {
	prevLogIndex := rf.nextIndex[peer] - 1
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      rf.log[(prevLogIndex + 1):],
		LeaderCommit: rf.commitIndex,
	}
	var reply AppendEntriesReply
	go func() {
		if ok := rf.sendAppendEntries(peer, &arg, &reply); !ok {
			DPrintf("Peer[%d] => Peer[%d]: AppendEntry failed", rf.me, peer)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("Peer[%d] => Peer[%d]: AppendEntry response received %+v", rf.me, peer, reply)
		if reply.Success {
			rf.nextIndex[peer] = arg.PrevLogIndex + len(arg.Entries) + 1
			rf.matchIndex[peer] = arg.PrevLogIndex + len(arg.Entries)
			rf.updateCommitIndex()
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
