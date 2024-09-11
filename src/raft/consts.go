package raft

import "time"

type Role int

const (
	follower Role = iota
	candidate
	leader
)

var CheckInterval time.Duration = time.Millisecond * 5
