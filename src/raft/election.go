package raft

import "time"

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
		time.Sleep(CheckInterval)
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
	// set to 1 since candidate votes for itself
	approveCount := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	// save the currentTerm
	currentTerm := rf.currentTerm

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

		// if election timeout while waiting for reply
		if time.Since(rf.electionTimeoutBaseline) > rf.electionTimeoutDuration {
			DPrintf("Peer[%d]: wait for reply timeout", rf.me)
			rf.mu.Unlock()
			return
		}

		// if a reply with higher term is received, turn to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = follower
			DPrintf("Peer[%d]: reply with higher term received, turning to follower", rf.me)
			rf.mu.Unlock()
			return
		}

		// if term changed while waiting for reply, abort election
		if currentTerm != rf.currentTerm {
			DPrintf("Peer[%d]: term changed while waiting, abort election", rf.me)
			rf.mu.Unlock()
			return
		}

		if reply.VoteGranted {
			approveCount += 1
		}
		DPrintf("Peer[%d]: approveCount = %d", rf.me, approveCount)

		// check if received majority of vote
		if approveCount > len(rf.peers)/2 {
			DPrintf("Peer[%d] turns to leader", rf.me)
			rf.leaderInitialization()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderInitialization() {
	rf.role = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}
	for peer := 0; peer < len(rf.peers); peer += 1 {
		if peer == rf.me {
			continue
		}
		rf.nextIndex[peer] = rf.getLastLogIndex() + 1
		rf.matchIndex[peer] = 0
		var rsp AppendEntriesReply
		go rf.sendAppendEntries(peer, &args, &rsp)
	}
}