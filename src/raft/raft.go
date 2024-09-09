package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role        Role
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine

	// used in election process
	electionTimeoutDuration time.Duration // random election timeout: 300 - 450ms
	electionTimeoutBaseline time.Time

	// time interval for leader to send appendEntry
	appendEntryDuration time.Duration
	// last time sending appendEntry for leader
	appendEntryBaseline time.Time

	// volatile states on leader
	nextIndex  []int // index of next log to send to each peer
	matchIndex []int // index of highest log entry known to be replicated on server

	applyChan chan ApplyMsg // the apply channel for the application layer
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = (rf.role == leader)
	if !isLeader {
		return
	}
	le := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, le)
	index = rf.getLastLogIndex()
	term = rf.currentTerm
	rf.persist()
	DPrintf("Peer[%d]: start consensus with command %v, logEntry %v, index %v", rf.me, command, le, index)

	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int = rf.currentTerm
	var isleader bool = (rf.role == leader)
	rf.mu.Unlock()
	return term, isleader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		role:        follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		applyChan:   applyCh,
	}
	rf.log[0].Command = nil
	rf.log[0].Term = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// initailize the timeout stats
	rf.resetElectionTimeoutDuration()
	rf.electionTimeoutBaseline = time.Now()
	rf.appendEntryBaseline = time.Now()
	rf.appendEntryDuration = 50 * time.Millisecond
	// start ticker goroutine to start elections
	go rf.electionRoutine()
	go rf.appendEntryRoutine()
	go rf.applyEntryRoutine()
	return rf
}
