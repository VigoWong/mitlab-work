package raft

import "time"

// AppendEntry RPC Call Handler
//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex
//whose term matches prevLogTerm (§5.3) TODO
//3. If an existing entry conflicts with a new one (same index but different terms),
// delete the existing entry and all that follow it (§5.3) TODO
//4. Append any new entries not already in the log TODO
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) TODO
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	DPrintf("「AppendEntry」%v\n", args)
	reply.Success = true
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	}

	switch rf.role {
	case Leader:
		if reply.Success {
			// recognise the new leader
			rf.role = Follower
		}
	case Candidate:
		if reply.Success {
			rf.role = Follower
		}
	}
	// Reset the timer
	rf.heartbeatTimer.Reset((HeartbeatInterval - 10) * time.Millisecond)
}

// sendAppendEntry SendHeartBeat Heartbeat Sender
func (rf *Raft) sendAppendEntry() {
	if rf.role == Leader {
		// raise election, call requestVote
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				arg := &AppendEntryArgs{
					// TODO
					Term:         rf.currentTerm,
					LeaderCommit: rf.commitIndex,
					LeaderID:     rf.me,
					PreLogIndex:  rf.nextIndex[i],
					PreLogTerm:   rf.logs[rf.nextIndex[i]].term,
					Entries:      nil,
				}
				reply := &AppendEntryReply{}
				rf.sendAppendEntryRPC(i, arg, reply)
			}
		}
		rf.heartbeatTimer.Reset((HeartbeatInterval) * time.Millisecond)
	}
}

// sendAppendEntryRPC
func (rf *Raft) sendAppendEntryRPC(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	DPrintf("「sendAppendEntryRPC」%v\n", args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
