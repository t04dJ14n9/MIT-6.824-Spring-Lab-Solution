package raft

type Role int

const (
	follower Role = iota
	candidate
	leader
)
