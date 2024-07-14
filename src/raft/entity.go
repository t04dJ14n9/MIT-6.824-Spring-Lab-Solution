package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateID  int
	lastLogIndex int
	lastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

type AppendEntriesArgs struct {
	term         int        // leader's term
	leaderID     int        // so follower can redirect clients
	prevLogIndex int        // index of log entry immediately preceding new ones
	prevLogTerm  int        // term of prevLogIndex entry
	entries      []LogEntry // log entries to store(empty for heartbeat; may send more than one for efficiency)
	leaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	term    int  // currentTerm, for leader to update itself
	success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}
