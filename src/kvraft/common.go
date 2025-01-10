package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRaft        = "ErrRaft"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqID    int
	ClientID int64 // used for uniquely determine a client
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	SeqID    int
	ClientID int64 // used for uniquely determine a client
}

type GetReply struct {
	Err   Err
	Value string
}

const (
	CheckInterval = 10 * time.Millisecond
)
