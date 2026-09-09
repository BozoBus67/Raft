package main

type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
	OpNoOp   Op = "noop" // leader submits one on election to force a current-term commit
)

// Log is 1-based: log[0] is a sentinel {Term:0}; real entries start at index 1.
type LogEntry struct {
	Term  int
	Op    Op
	Key   string
	Value string
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Backup optimization (paper §5.3, optional but included).
	// On failure the follower reports where the leader should back off to.
	ConflictIndex int
	ConflictTerm  int
}
