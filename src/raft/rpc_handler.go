package raft

import "time"

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Peer[%d]: RequestVote received: %+v", rf.me, *args)

	// if RPC request with higher term received, convert to follower
	if args.Term > rf.currentTerm {
		DPrintf("Peer[%d]: RequestVote with a higher term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		rf.role = follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = args.Term
		reply.VoteGranted = true
		DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
		return
	}

	// reject requests with smaller term number
	if args.Term < rf.currentTerm {
		DPrintf("Peer[%d]: RequestVote with smaller term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID { //&& // didn't vote for anyone else and \n
		// args.Term > rf.currentTerm || (args.Term == rf.currentTerm && args.LastLogIndex >= rf.getLastLogIndex()) { // candidate's log is more up-to-date
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.resetElectionTimeoutDuration()
		rf.electionTimeoutBaseline = time.Now()
		reply.Term = args.Term
		reply.VoteGranted = true
		DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
	return
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		DPrintf("Peer[%d]: AppendEntries with a higher term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		rf.role = follower
		rf.currentTerm = args.Term
		reply.Success = true
		reply.Term = args.Term
		DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
		return
	}
	if args.Term < rf.currentTerm {
		DPrintf("Peer[%d]: AppendEntries with smaller term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
		return
	}
	// term number is the same as rf.currentTerm, receiving vote from current leader

	// if rf.getLastLogIndex() < args.PrevLogIndex ||
	// 	(rf.getLastLogIndex() >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.Term) {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
	// 	return
	// }
	if rf.role == candidate {
		DPrintf("Peer[%d]: candidate to follower", rf.me)
		rf.role = follower
	}
	rf.electionTimeoutBaseline = time.Now()
	rf.resetElectionTimeoutDuration()
	// save log
	if rf.getLastLogIndex() != args.PrevLogIndex {
		DPrintf("Peer[%d]: AppendEntry PrevLogEntry = %d does not match lastLogIndex = %d", rf.me, args.PrevLogIndex, rf.getLastLogIndex())
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if len(args.Entries) != 0 {
		DPrintf("Peer[%d]: Append Entries to log: %+v", rf.me, args.Entries)
	}
	DPrintf("Peer[%d] log %v", rf.me, rf.log)
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("Peer[%d]: updating commitIndex: %d => %d", rf.me, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
	return
}
