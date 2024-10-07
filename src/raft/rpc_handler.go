package raft

import "time"

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logMsg := ""
	defer func() {
		logMsg = AddToLogMsg(logMsg, "Peer[%d] => Peer[%d]: RequestVote reply = %+v", rf.me, args.CandidateID, reply)
		DPrint(logMsg)
	}()
	logMsg = AddToLogMsg(logMsg, "Peer[%d] => Peer[%d]: RequestVote received: %+v, lastIncludedIndex=%v, lastIncludedTerm=%v",
		args.CandidateID, rf.me, *args, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// if RPC request with higher term received, convert to follower
	if args.Term > rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: RequestVote with a higher term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		rf.role = follower
		rf.currentTerm = args.Term
		// does not reset election timeout to avoid endless loop
		rf.votedFor = -1
		rf.persist()
	}

	// reject requests with smaller term number
	if args.Term < rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: RequestVote with smaller term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if already voted for this candidate, grant vote
	if rf.votedFor == args.CandidateID {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: already voted for same candidate: %d, grant vote", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// if already voted for someone else, reject
	if rf.votedFor != -1 {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: already voted for someone else: %d, reject RequestVote", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate's log is not as up-to-date as receiver's log, reject
	if args.LastLogTerm == rf.getLastLogicalLogTerm() && args.LastLogIndex < rf.getLastLogicalLogIndex() || // same term, higher last index means more up-to-date
		args.LastLogTerm < rf.getLastLogicalLogTerm() { // different term, higher term means more up-to-date
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: candidate's log is not as up-to-date as receiver's log, reject", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// vote granted
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateID
	rf.persist()
	logMsg = AddToLogMsg(logMsg, "Peer[%d]: resetting election timeout for granting vote", rf.me)
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
		logMsg = AddToLogMsg(logMsg, "Peer[%d] => Peer[%d]: AppendEntry reply = %+v", rf.me, args.LeaderID, reply)
		DPrint(logMsg)
	}()
	logMsg = AddToLogMsg(logMsg, "Peer[%d] => Peer[%d]: Receive AppendEntries: %+v, lastIncludedIndex=%v, lastIncludedTerm=%v",
		args.LeaderID, rf.me, args, rf.lastIncludedIndex, rf.lastIncludedTerm)

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: AppendEntries with a higher term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		rf.role = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: AppendEntries with smaller term received. Current Term: %v Received Term: %v", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.role == leader {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: reject appendEntries with same term as leader", rf.me)
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

	logMsg = AddToLogMsg(logMsg, "Peer[%d]: log=%v, lastIncludedIndex=%v, lastIncludedTerm=%v", rf.me, rf.log, rf.lastIncludedIndex, rf.lastIncludedTerm)
	if !(rf.lastIncludedIndex == args.PrevLogIndex && rf.lastIncludedTerm == args.PrevLogTerm) {
		// does not contain an index at prevLogIndex
		if rf.LogicIndex2RealIndex(args.PrevLogIndex) < 0 || rf.LogicIndex2RealIndex(args.PrevLogIndex) >= len(rf.log) {
			logMsg = AddToLogMsg(logMsg, "Peer[%d]: does not contain an entry at prevLogIndex=%d", rf.me,
				args.PrevLogIndex)
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = rf.getLastLogicalLogIndex()
			reply.ConflictTerm = -1 // indicating non-existent
			return
		}

		// contain an index at prevLogIndex but term does not match
		if rf.log[rf.LogicIndex2RealIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
			logMsg = AddToLogMsg(logMsg, "Peer[%d]: does not contain an entry at prevLogIndex whose term matches prevLogTerm. prevLogIndex: %d, prevLogTerm: %d", rf.me,
				args.PrevLogIndex, args.PrevLogTerm)
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = rf.log[rf.LogicIndex2RealIndex(args.PrevLogIndex)].Term
			// set conflictIndex to be the index of the first entry with ConflictTerm
			conflictIndex := args.PrevLogIndex
			for rf.LogicIndex2RealIndex(conflictIndex) >= 0 && rf.log[rf.LogicIndex2RealIndex(conflictIndex)].Term == reply.ConflictTerm {
				conflictIndex--
			}
			reply.ConflictIndex = conflictIndex + 1
			return
		}
	}
	logMsg = AddToLogMsg(logMsg, "log before: %v", rf.log)

	// if an existing entry conflicts with a new one, delete the existing entry and all that follow it
	for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+len(args.Entries)+1 && i <= rf.getLastLogicalLogIndex(); i++ {
		// Log Matching Property ensures that if index and term are the same, all log entries up through this index is the same
		if args.Entries[i-args.PrevLogIndex-1].Term != rf.log[rf.LogicIndex2RealIndex(i)].Term {
			logMsg = AddToLogMsg(logMsg, "Peer[%d]: remove log after %d-th index", rf.me, i)
			rf.log = append([]LogEntry{}, rf.log[:rf.LogicIndex2RealIndex(i)]...)
			rf.persist()
		}
	}
	// Append log entries in args
	rf.applyEntriesToLog(args.Entries, args.PrevLogIndex)

	logMsg = AddToLogMsg(logMsg, "log after: %v", rf.log)
	rf.persist()

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		logMsg = AddToLogMsg(logMsg, "Peer[%d]: updating commitIndex: %d => %d", rf.me, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(rf.getLastLogicalLogIndex(), args.LeaderCommit)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

// idempotent application of log entries in the request to local log
func (rf *Raft) applyEntriesToLog(entries []LogEntry, prevLogIndex int) {
	for i := 0; i < len(entries); i++ {
		indexToInsert := prevLogIndex + i + 1
		if rf.LogicIndex2RealIndex(indexToInsert) >= len(rf.log) {
			rf.log = append(rf.log, entries[i])
			continue
		}
		rf.log[rf.LogicIndex2RealIndex(indexToInsert)] = entries[i]
	}
	return
}

func (rf *Raft) InstallSnapshot(arg *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logMsg := ""
	defer func() {
		logMsg = AddToLogMsg(logMsg, "InstallSnapshot reply = %+v", reply)
		DPrint(logMsg)
	}()
	logMsg = AddToLogMsg(logMsg, "Peer[%d] => Peer[%d]: InstallSnapshot args: %+v", arg.LeaderID, rf.me, arg)

	reply.Term = rf.currentTerm

	if arg.Term < rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "arg.Term=%v < rf.currentTerm=%v, reject.", arg.Term, rf.currentTerm)
		return
	}

	if arg.Term > rf.currentTerm {
		logMsg = AddToLogMsg(logMsg, "updating term: %v => %v", rf.currentTerm, arg.Term)
		rf.role = follower
		rf.currentTerm = arg.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.electionTimeoutBaseline = time.Now()
	rf.resetElectionTimeoutDuration()

	// reject old snapshot
	if arg.LastIncludedIndex <= rf.commitIndex {
		logMsg = AddToLogMsg(logMsg, "commitIndex=%v >= arg.lastIncludedIndex=%v", rf.commitIndex, arg.LastIncludedIndex)
		return
	}

	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: arg.LastIncludedIndex,
		SnapshotTerm:  arg.LastIncludedTerm,
		Snapshot:      arg.Snapshot,
	}
	logMsg = AddToLogMsg(logMsg, "Peer[%d]: applyMsg sent %+v", rf.me, snapshotMsg)

	go func() {
		rf.applyChan <- snapshotMsg
	}()
	return
}
