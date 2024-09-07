package raft

import "time"

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logMsg := ""
	defer func() {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: RequestVote reply = %+v", rf.me, reply)
		DPrint(logMsg)
	}()
	logMsg = AddToLogMsg(logMsg, "Peer[%d]: RequestVote received: %+v", rf.me, *args)

	// if RPC request with higher term received, convert to follower
	if args.Term > rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: RequestVote with a higher term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		rf.role = follower
		rf.currentTerm = args.Term
		// does not reset election timeout to avoid endless loop
		rf.votedFor = -1
	}

	// reject requests with smaller term number
	if args.Term < rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: RequestVote with smaller term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if already voted for someone else, reject
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: already voted for %d, reject RequestVote", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate's log is not as up-to-date as receiver's log, reject
	if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex() || // same term, higher last index means more up-to-date
		args.LastLogTerm < rf.getLastLogTerm() { // different term, higher term means more up-to-date
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: candidate's log is not as up-to-date as receiver's log, reject", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// vote granted
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateID
	rf.resetElectionTimeoutDuration()
	rf.electionTimeoutBaseline = time.Now()
	reply.Term = args.Term
	reply.VoteGranted = true
	return
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logMsg := ""
	defer func() {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: AppendEntry reply = %+v", rf.me, reply)
		DPrint(logMsg)
	}()
	if args.Term > rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: AppendEntries with a higher term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		rf.role = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if args.Term < rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: AppendEntries with smaller term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.role == candidate {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: candidate to follower", rf.me)
		rf.role = follower
	}

	// reset election timeout first and check whether reject or accept request because this appendEntry is from current leader
	// reset election timeout only when:
	// 1. begin another round of election (follower timeout or candidate start another round of election)
	// 2. receive appendEntry RPC only from leader of **current term**
	rf.electionTimeoutBaseline = time.Now()
	rf.resetElectionTimeoutDuration()

	// term number is the same as rf.currentTerm
	// reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.getLogLength()-1 < args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: does not contain an entry at prevLogIndex whose term matches prevLogTerm. prevLogIndex: %d, prevLogTerm: %d, log: %v", rf.me,
			args.PrevLogIndex, args.PrevLogTerm, rf.log)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if len(args.Entries) != 0 {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: Append Entries to log: %+v", rf.me, args.Entries)
	}
	logMsg = AddToLogMsg(logMsg, "Peer[%d]: log before: %v", rf.me, rf.log)

	// if an existing entry conflicts with a new one, delete the existing entry and all that follow it
	for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+len(args.Entries)+1 && i < rf.getLogLength(); i++ {
		// Log Matching Property ensures that if index and term are the same, all log entries up through this index is the same
		if args.Entries[i-args.PrevLogIndex-1].Term != rf.log[i].Term { //|| args.Entries[i-args.PrevLogIndex-1].Command != rf.log[i].Command {
			logMsg = AddToLogMsg(logMsg, "Peer[%d]: remove log after %d-th index", rf.me, i)
			rf.log = rf.log[:i]
		}
	}
	// Append log entries in args
	rf.log = append(rf.log, args.Entries...)
	logMsg = AddToLogMsg(logMsg, "Peer[%d]: log after: %v", rf.me, rf.log)

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: updating commitIndex: %d => %d", rf.me, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(rf.getLastLogIndex(), args.LeaderCommit)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}
