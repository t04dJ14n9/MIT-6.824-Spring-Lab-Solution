package raft

import "time"

// The electionRoutine go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionRoutine() {
	// check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
	for !rf.killed() {
		rf.mu.Lock()
		if time.Since(rf.electionTimeoutBaseline) > rf.electionTimeoutDuration {
			if rf.role != leader {
				rf.doElection()
				time.Sleep(CheckInterval)
				continue
			} else {
				rf.electionTimeoutBaseline = time.Now()
			}
		}
		rf.mu.Unlock()
		time.Sleep(CheckInterval)
	}
}

func (rf *Raft) doElection() {
	DPrintf("Peer[%d] doElection, resetting electionTimout", rf.me)
	rf.currentTerm += 1
	rf.role = candidate
	rf.electionTimeoutBaseline = time.Now()
	rf.resetElectionTimeoutDuration()
	rf.votedFor = rf.me
	rf.persist()
	// number of peers that approve the vote in current round of election
	// set to 1 since candidate votes for itself
	approveCount := 1
	receivedCount := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastLogicalLogIndex(),
		LastLogTerm:  rf.getLastLogicalLogTerm(),
	}

	// save the currentTerm
	savedTerm := rf.currentTerm

	rf.mu.Unlock()

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go func() {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			logMsg := ""
			defer func() { DPrint(logMsg) }()

			receivedCount += 1
			if !ok {
				logMsg = AddToLogMsg(logMsg, "Peer[%d] => Peer[%d]: RequestVote failed", rf.me, peer)
				return
			}
			logMsg = AddToLogMsg(logMsg, "Peer[%d]: received reply %+v", rf.me, reply)

			// if a reply with higher term is received, turn to follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = follower
				rf.votedFor = -1
				rf.persist()
				logMsg = AddToLogMsg(logMsg, "Peer[%d]: reply with higher term received. candidate => follower", rf.me)
				return
			}

			// if no longer candidate while waiting for requestVote reply,
			if rf.role != candidate {
				logMsg = AddToLogMsg(logMsg, "Peer[%d]: not candidate when receiving reply", rf.me)
				return
			}

			// if term changed while waiting for reply, abort election
			if savedTerm != rf.currentTerm {
				logMsg = AddToLogMsg(logMsg, "Peer[%d]: term changed while waiting, abort election", rf.me)
				return
			}

			if reply.VoteGranted {
				approveCount += 1
			}
			logMsg = AddToLogMsg(logMsg, "Peer[%d]: approveCount = %d", rf.me, approveCount)

			// if received majority of vote
			if approveCount > len(rf.peers)/2 {
				logMsg = AddToLogMsg(logMsg, "Peer[%d]: wins election. candidate => leader.", rf.me)
				rf.leaderInitialization()
				return
			}

			// did not receive the majority of vote and all replies was received
			if receivedCount == len(rf.peers) {
				logMsg = AddToLogMsg(logMsg, "Peer[%d]: received all requestVote replies but did not got majority of vote.", rf.me)
				return
			}
		}()
	}
}

func (rf *Raft) leaderInitialization() {
	rf.role = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize nextIndex and matchIndex for each peer
	for peer := 0; peer < len(rf.peers); peer += 1 {
		rf.nextIndex[peer] = rf.getLastLogicalLogIndex() + 1
		rf.matchIndex[peer] = 0
	}
}
