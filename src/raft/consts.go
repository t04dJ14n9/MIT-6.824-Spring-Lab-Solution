package raft

import "time"

type Role int

const (
	follower Role = iota
	candidate
	leader
)

var ElectionCheckInterval time.Duration = time.Millisecond * 10
var AppendEntryCheckInterval time.Duration = time.Millisecond * 10
