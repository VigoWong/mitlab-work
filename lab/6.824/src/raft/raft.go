package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// eventLoop starts a new election if this peer hasn't received heartbeats recently.
// The main event event loop of a peer
func (rf *Raft) eventLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		select {
		case <-rf.electionTimer.C:
			// election time out
			rf.HandlerElectionTimeout()
		case <-rf.heartbeatTimer.C:
			// heart beat time out
			rf.HandleHeartBeatTimeout()
		}
		rf.mu.Unlock()
	}
}

/* ---------------------------- Election timeout ---------------------------- */

/* ---------------------------- Election timeout ---------------------------- */

/* ---------------------------- Heartbeat timeout ---------------------------- */

func (rf *Raft) HandleHeartBeatTimeout() {
	switch rf.role {
	case Leader:
		// Send out Append Entry
		rf.transitToLeader()
	case Candidate:
		DPrintf("「HandleHeartBeatTimeout」Candidate Got a HeartBeatTimeout")
	case Follower:
		// Start of the program, follower's heartbeat expire transit to candidate and trigger election timeout
		rf.transitToCandidate()
	}
}

func (rf *Raft) transitToCandidate() {
	// transit to candidate
	rf.role = Candidate
	rf.currentTerm++

	// trigger election timeout
	rf.electionTimer.Reset(RandomiseDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
}

func (rf *Raft) transitToLeader() {
	// transit to candidate
	rf.role = Leader
	rf.sendAppendEntry()
}

/* ---------------------------- Heartbeat timeout -------------------------------- */

// Make ：the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		electionTimer:  time.NewTimer(RandomiseDuration(ElectionTimeoutLower, ElectionTimeoutUpper)),
		heartbeatTimer: time.NewTimer(HeartbeatInterval * time.Millisecond),
		currentTerm:    0,
		votedFor:       -1,           // -1 for null
		logs:           []*RaftLog{}, // TODO
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      nil,
		matchIndex:     nil,
		role:           Follower, // Peers are initialized as follower
	}

	// initialize from state persisted before a crash, restore the logs above
	rf.readPersist(persister.ReadRaftState())

	// start eventLoop goroutine to start elections
	go rf.eventLoop()

	return rf
}
