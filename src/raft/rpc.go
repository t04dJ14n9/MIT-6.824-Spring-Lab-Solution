package raft

import "time"

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
	defer rf.mu.Unlock()

	DPrintf("Peer[%d]: RequestVote received: %+v", rf.me, *args)
	if args.Term > rf.currentTerm { // if RPC request with higher term received, convert to follower
		DPrintf("Peer[%d]: RequestVote with a higher term received", rf.me)
		rf.role = follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = args.Term
		reply.VoteGranted = true
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID { //&& // didn't vote for anyone else and \n
		// args.Term > rf.currentTerm || (args.Term == rf.currentTerm && args.LastLogIndex >= rf.getLastLogIndex()) { // candidate's log is more up-to-date
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		DPrintf("Peer[%d]: AppendEntry with higher term received, convert to follower", rf.me)
		rf.role = follower
		rf.currentTerm = args.Term
		reply.Success = true
		reply.Term = args.Term
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
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
