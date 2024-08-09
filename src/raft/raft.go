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
	leaderID    int
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
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Peer[%d]: RequestVote received: %+v", rf.me, *args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID { //&& // didn't vote for anyone else and \n
		// args.Term > rf.currentTerm || (args.Term == rf.currentTerm && args.LastLogIndex >= rf.getLastLogIndex()) { // candidate's log is more up-to-date
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
		return
	}
	// if rf.getLastLogIndex() < args.PrevLogIndex ||
	// 	(rf.getLastLogIndex() >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.Term) {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
	// 	return
	// }
	// fresh election timeout
	if rf.role != follower {
		DPrintf("Peer[%d] turns to follower", rf.me)
	}
	if rf.role == follower {
		rf.electionTimeoutBaseline = time.Now()
	}
	rf.role = follower
	reply.Term = rf.currentTerm
	reply.Success = true
	DPrintf("Peer[%d] -> Peer[%d]: AppendEntry reply = %+v", rf.me, args.LeaderID, reply)
	return
	// TODO: complete implementation
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	// Your code here, if desired.
}

func (rf *Raft) appendEntryRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == leader && time.Since(rf.appendEntryBaseline) > rf.appendEntryDuration {
			rf.appendEntryBaseline = time.Now()
			arg := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.getLastLogIndex(),
			}
			rf.mu.Unlock()
			for peer := 0; peer < len(rf.peers); peer++ {
				if peer == rf.me {
					continue
				}
				var reply AppendEntriesReply
				go rf.sendAppendEntries(peer, &arg, &reply)
			}
			goto SLEEP
		}
		rf.mu.Unlock()
	SLEEP:
		time.Sleep(AppendEntryCheckInterval)
	}
}

func (rf *Raft) doElection() {
	DPrintf("Peer[%d] election timeout", rf.me)
	rf.currentTerm += 1
	rf.role = candidate
	rf.electionTimeoutBaseline = time.Now()
	rf.resetElectionTimeoutDuration()
	rf.votedFor = rf.me
	// number of peers that approve the vote in current round of election
	approveCount := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	// send requestVote to all peers
	replyChannel := make(chan RequestVoteReply, len(rf.peers))
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go rf.sendRequestVote(peer, &args, replyChannel)
	}

	for {
		reply := <-replyChannel
		rf.mu.Lock()
		DPrintf("Peer[%d]: received reply %+v", rf.me, reply)
		// if while waiting for the reply, an appendEntry RPC with equal or higher term is received,
		// turn to follower and reset election timeout
		if rf.role == follower {
			DPrintf("Peer[%d]: already turned to follower while receiving reply", rf.me)
			rf.mu.Unlock()
			return
		}
		if time.Since(rf.electionTimeoutBaseline) > rf.electionTimeoutDuration {
			DPrintf("Peer[%d]: wait for reply timeout", rf.me)
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm { // if a reply with higher term is received, turn to follower
			rf.currentTerm = reply.Term
			rf.role = follower
			DPrintf("Peer[%d]: reply with higher term received, turning to follower", rf.me)
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			approveCount += 1
		}
		DPrintf("Peer[%d]: approveCount = %d", rf.me, approveCount)
		if approveCount > len(rf.peers)/2 {
			DPrintf("Peer[%d] turns to leader", rf.me)
			rf.leaderInitialization()
			return
		}
	}
}

// The electionRoutine go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionRoutine() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != leader && time.Since(rf.electionTimeoutBaseline) > rf.electionTimeoutDuration { // election starts
			rf.doElection()
			goto SLEEP
		}
		rf.mu.Unlock()
	SLEEP:
		time.Sleep(ElectionCheckInterval)
	}
}

func (rf *Raft) leaderInitialization() {
	rf.role = leader
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}
	rf.mu.Unlock()
	for peer := 0; peer < len(rf.peers); peer += 1 {
		if peer == rf.me {
			continue
		}
		var rsp AppendEntriesReply
		go rf.sendAppendEntries(peer, &args, &rsp)
	}
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
		leaderID:    -1,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
	}
	rf.log[0].Command = nil
	rf.log[0].Term = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// initailize the timeout stats
	rf.resetElectionTimeoutDuration()
	rf.electionTimeoutBaseline = time.Now()
	rf.appendEntryBaseline = time.Now()
	rf.appendEntryDuration = 120 * time.Millisecond
	// start ticker goroutine to start elections
	go rf.electionRoutine()
	go rf.appendEntryRoutine()
	return rf
}
