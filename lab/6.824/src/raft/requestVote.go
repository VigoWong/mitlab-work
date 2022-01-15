package raft

// RequestVote example RequestVote RPC handler. So it's a handler here
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("「RequestVote」%v\n", args)
	//1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}
	//2. If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		args.LastLogIndex >= rf.commitIndex {
		// TODO grant vote
		reply.VoteGranted = true
	} else {
		// TODO do not grant vote
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// sendRequestVote
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("「sendRequestVote」%v\n", args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) HandlerElectionTimeout() {
	if rf.role == Candidate {
		// raise election, call requestVote
		for i := 0; i < len(rf.peers); i++ {
			votes := 1
			if i != rf.me {
				arg := &RequestVoteArgs{
					Term:         0,
					CandidateID:  0,
					LastLogIndex: 0,
					LastLogTerm:  0,
				}
				reply := &RequestVoteReply{}
				rf.sendRequestVote(i, arg, reply)

				// analyse the reply
				if reply.VoteGranted {
					votes++
				}
			}
			if IsMajority(votes, len(rf.peers)) {
				// TODO transit to leader
				rf.sendAppendEntry()
			} else {
				rf.electionTimer.Reset(RandomiseDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
			}
		}
	}
}
