package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

// Raft a Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill(), 0 - run & 1 - stopped

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int64      // candidateId that received vote in current term (or -1 if none)
	logs        []*RaftLog // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int        // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int        // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	role       Role  // current role of the peer
}

type RaftLog struct {
	index   int
	term    int
	command struct{} // TODO
}

// ApplyMsg each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or tester).
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

/* -------------------- RequestVote --------------------*/

//RequestVoteArgs argument of RequestVote RPC call
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int   // Candidate's term
	CandidateID  int64 // Candidate's ID(pid for this case)
	LastLogIndex int   // index of candidate???s last log entry
	LastLogTerm  int   // term of candidate???s last log entry
}

//RequestVoteReply Reply of RequestVote RPC call
type RequestVoteReply struct {
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // If the candidate receive vote
}

/* -------------------- RequestVote --------------------*/

/* -------------------- AppendEntry --------------------*/

//AppendEntryArgs argument of AppendEntry RPC call
type AppendEntryArgs struct {
	// Your data here (2A, 2B).
	Term         int        // leader???s term
	LeaderCommit int        // leader???s commitIndex
	LeaderID     int        // so follower can redirect clients
	PreLogIndex  int        // index of log entry immediately preceding new ones
	PreLogTerm   int        // term of prevLogIndex entry
	Entries      []*RaftLog // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

//AppendEntryReply Reply of AppendEntry RPC call
type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

/* -------------------- AppendEntry --------------------*/
