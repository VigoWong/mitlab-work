package raft

type Role int

const (
	Leader = iota + 1
	Follower
	Candidate
)

const HeartbeatInterval = 120
const ElectionTimeoutLower = 300
const ElectionTimeoutUpper = 400
