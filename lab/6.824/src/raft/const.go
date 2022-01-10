package raft

import "time"

type Role int

const (
	Leader = iota + 1
	Follower
	Candidate
)

const HeartbeatInterval = time.Duration(120) * time.Millisecond
const ElectionTimeoutLower = time.Duration(300) * time.Millisecond
const ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
