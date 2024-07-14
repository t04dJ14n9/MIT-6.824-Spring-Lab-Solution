package raft

type Role int

const (
	follower Role = iota
	candidate
	leader
)

var minElectionTimeout int = 150
var maxElectionTimeout int = 300
