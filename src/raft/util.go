package raft

import (
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

// Get the last index of self's log, to be modified to adapt to snapshot
// TODO: if add lock around res it cannot proceed
func (rf *Raft) getLastLogIndex() int {
	res := len(rf.log) - 1
	return res
}

// Get the last term of self's log, to be modified to adapt to snapshot
// TODO: if add lock around res it cannot proceed
func (rf *Raft) getLastLogTerm() int {
	res := rf.log[rf.getLastLogIndex()].Term
	return res
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyChannel chan<- RequestVoteReply) {
	var reply RequestVoteReply
	DPrintf("Peer[%d]: sendRequestVote to Peer[%d]. req = %+v", rf.me, server, *args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		replyChannel <- RequestVoteReply{Term: 0, VoteGranted: false}
	}
	replyChannel <- reply
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Peer[%d] -> Peer[%d]: AppendEntry args %+v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// resetElectionTimeoutDuration resets electionTimeoutDuration to a random value between 300 - 450 milliseconds
func (rf *Raft) resetElectionTimeoutDuration() {
	rf.electionTimeoutDuration = time.Duration(rand.Intn(300)+150) * time.Millisecond
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
