package raft

import "time"

type Role int

const (
	follower Role = iota
	candidate
	leader
)

var CheckInterval time.Duration = time.Millisecond * 10

var ElectionTimeoutLow int = 300

var ElectionTimeoutHigh int = 450

var AppendEntryInterval time.Duration = time.Millisecond * 50
