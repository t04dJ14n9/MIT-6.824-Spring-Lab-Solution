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

	// if already voted for someone else, reject
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		DPrintf("Peer[%d]: already voted for %d, reject RequestVote", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
		return
	}

	// if candidate's log is not as up-to-date as receiver's log, reject
	if args.Term == rf.currentTerm && args.LastLogIndex < rf.getLastLogIndex() || // same term, higher last index means more up-to-date
		args.Term < rf.currentTerm { // different term, higher term means more up-to-date
		DPrintf("candidate's log is not as up-to-date as receiver's log, reject")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Peer[%d]: RequestVote response: %+v", rf.me, *reply)
		return
	}

	// vote granted
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateID
	rf.resetElectionTimeoutDuration()
	rf.electionTimeoutBaseline = time.Now()
	reply.Term = args.Term
	reply.VoteGranted = true
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

	// term number is the same as rf.currentTerm
	// reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.getLogLength()-1 < args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Peer[%d]: does not contain an entry at prevLogIndex whose term matches prevLogTerm. prevLogIndex: %d, prevLogTerm: %d, log: %v", rf.me,
			args.PrevLogIndex, args.PrevLogTerm, rf.log)
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
		return
	}

	if rf.role == candidate {
		DPrintf("Peer[%d]: candidate to follower", rf.me)
		rf.role = follower
	}
	rf.electionTimeoutBaseline = time.Now()
	rf.resetElectionTimeoutDuration()

	if len(args.Entries) != 0 {
		DPrintf("Peer[%d]: Append Entries to log: %+v", rf.me, args.Entries)
	}
	DPrintf("Peer[%d] log %v", rf.me, rf.log)

	// if an existing entry conflicts with a new one, delete the existing entry and all that follow it
	for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+len(args.Entries)+1 && i < rf.getLogLength(); i++ {
		if args.Entries[i-args.PrevLogIndex-1].Term != rf.log[i].Term || args.Entries[i-args.PrevLogIndex-1].Command != rf.log[i].Command {
			DPrintf("Peer[%d]: remove log after %d-th index", rf.me, i)
			rf.log = rf.log[:i]
		}
	}
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
