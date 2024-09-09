package raft

import "time"

type Role int

const (
	follower Role = iota
	candidate
	leader
)

var CheckInterval time.Duration = time.Millisecond * 5

const (
	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Purple = "\033[35m"
	Cyan   = "\033[36m"
	White  = "\033[37m"
)
