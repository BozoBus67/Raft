package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	Dead
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	}
	return "?"
}

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimerTick  = 10 * time.Millisecond
	minElectionTimeout = 250 * time.Millisecond
	maxElectionTimeout = 500 * time.Millisecond
	rpcDialTimeout     = 100 * time.Millisecond
	rpcCallTimeout     = 200 * time.Millisecond
)

type Node struct {
	mu sync.Mutex

	// identity + cluster
	id    int
	addr  string
	peers map[int]string

	// persistent (fsync'd on change)
	currentTerm int
	votedFor    int // -1 = none
	log         []LogEntry

	// volatile: all servers
	commitIndex int // 0 = nothing committed
	lastApplied int // 0 = nothing applied
	role        Role

	// volatile: leader only
	nextIndex  map[int]int
	matchIndex map[int]int

	// election bookkeeping
	electionResetAt time.Time
	electionTimeout time.Duration

	// internal wakeups (both buffered(1), non-blocking send pattern)
	triggerReplicateCh chan struct{}
	commitReadyCh      chan struct{}

	// storage
	storagePath string

	// state machine, mutated only under n.mu by applier or Submit
	kv map[string]string

	// RPC listener + shutdown
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewNode(id int, addr string, peerIDs map[int]string, storageDir string) *Node {
	n := &Node{
		id:                 id,
		addr:               addr,
		peers:              copyPeerMap(peerIDs),
		currentTerm:        0,
		votedFor:           -1,
		log:                []LogEntry{{Term: 0}}, // sentinel at index 0
		commitIndex:        0,
		lastApplied:        0,
		role:               Follower,
		nextIndex:          map[int]int{},
		matchIndex:         map[int]int{},
		triggerReplicateCh: make(chan struct{}, 1),
		commitReadyCh:      make(chan struct{}, 1),
		storagePath:        stateFilePath(storageDir, id),
		kv:                 map[string]string{},
		stopCh:             make(chan struct{}),
	}
	n.mu.Lock()
	n.loadPersist()
	n.resetElectionTimeout()
	n.mu.Unlock()
	return n
}

func copyPeerMap(src map[int]string) map[int]string {
	out := make(map[int]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// Serve starts the RPC listener and background loops.
func (n *Node) Serve() error {
	dispatcher := rpc.NewServer()
	if err := dispatcher.RegisterName("Node", n); err != nil {
		return err
	}
	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	n.listener = l
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return // listener closed on shutdown
			}
			go dispatcher.ServeConn(conn)
		}
	}()

	n.wg.Add(1)
	go n.electionTimerLoop()

	n.wg.Add(1)
	go n.applierLoop()

	log.Printf("[node %d] listening on %s term=%d", n.id, n.addr, n.currentTerm)
	return nil
}

// Shutdown stops the node. Idempotent.
func (n *Node) Shutdown() {
	n.mu.Lock()
	if n.role == Dead {
		n.mu.Unlock()
		return
	}
	n.role = Dead
	close(n.stopCh)
	n.mu.Unlock()

	if n.listener != nil {
		_ = n.listener.Close()
	}
	n.wg.Wait()
	log.Printf("[node %d] shut down", n.id)
}

// becomeFollower is called with n.mu held.
func (n *Node) becomeFollower(term int) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1
	}
	n.role = Follower
	n.resetElectionTimeout()
	n.savePersist()
}

// resetElectionTimeout picks a fresh randomized timeout AND resets the clock.
// Called with n.mu held.
func (n *Node) resetElectionTimeout() {
	span := maxElectionTimeout - minElectionTimeout
	n.electionTimeout = minElectionTimeout + time.Duration(rand.Int63n(int64(span)))
	n.electionResetAt = time.Now()
}

func (n *Node) electionTimerLoop() {
	defer n.wg.Done()
	t := time.NewTicker(electionTimerTick)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		if n.role == Dead || n.role == Leader {
			n.mu.Unlock()
			continue
		}
		if time.Since(n.electionResetAt) >= n.electionTimeout {
			n.startElectionLocked()
		}
		n.mu.Unlock()
	}
}

// startElectionLocked runs with n.mu held. It flips to Candidate, bumps term,
// self-votes, persists, then fires off RequestVote RPCs in the background.
func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimeout()
	n.savePersist()
	term := n.currentTerm
	lastIdx := len(n.log) - 1
	lastTerm := n.log[lastIdx].Term
	peers := copyPeerMap(n.peers)
	log.Printf("[node %d] --> election term=%d lastIdx=%d lastTerm=%d", n.id, term, lastIdx, lastTerm)

	votes := 1 // self
	needed := (len(n.peers)+1)/2 + 1

	for peerID, peerAddr := range peers {
		go func(peerID int, peerAddr string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			if !callRPC(peerAddr, "Node.RequestVote", &args, &reply) {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()

			// Stale reply. We've moved on.
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.Term > n.currentTerm {
				log.Printf("[node %d] election term=%d saw higher term %d in vote reply, stepping down",
					n.id, term, reply.Term)
				n.becomeFollower(reply.Term)
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes++
			log.Printf("[node %d] election term=%d got vote from %d (%d/%d)", n.id, term, peerID, votes, needed)
			if votes >= needed {
				n.becomeLeaderLocked()
			}
		}(peerID, peerAddr)
	}
}

// becomeLeaderLocked runs with n.mu held.
func (n *Node) becomeLeaderLocked() {
	if n.role != Candidate {
		return
	}
	n.role = Leader
	lastIdx := len(n.log) - 1
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastIdx + 1
		n.matchIndex[peerID] = 0
	}
	log.Printf("[node %d] === LEADER term=%d ===", n.id, n.currentTerm)

	// Submit a no-op so the current-term-commit rule (§5.4.2) has something
	// current-term to count toward, letting older entries commit indirectly.
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Op: OpNoOp})
	n.savePersist()

	term := n.currentTerm
	n.wg.Add(1)
	go n.leaderLoop(term)
	n.signalReplicate()
}

// leaderLoop drives heartbeats and on-demand replication for a given term. It
// exits when this node stops being leader at `term`.
func (n *Node) leaderLoop(term int) {
	defer n.wg.Done()
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		n.mu.Lock()
		if n.role != Leader || n.currentTerm != term || n.role == Dead {
			n.mu.Unlock()
			return
		}
		peers := copyPeerMap(n.peers)
		n.mu.Unlock()

		for peerID, peerAddr := range peers {
			go n.sendAppendEntries(peerID, peerAddr, term)
		}

		select {
		case <-n.stopCh:
			return
		case <-t.C:
		case <-n.triggerReplicateCh:
		}
	}
}

func (n *Node) sendAppendEntries(peerID int, peerAddr string, term int) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peerID]
	if next < 1 {
		next = 1
	}
	prevIdx := next - 1
	prevTerm := n.log[prevIdx].Term
	entries := make([]LogEntry, len(n.log)-next)
	copy(entries, n.log[next:])
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	var reply AppendEntriesReply
	if !callRPC(peerAddr, "Node.AppendEntries", &args, &reply) {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		log.Printf("[node %d] AE reply from %d has higher term %d, stepping down",
			n.id, peerID, reply.Term)
		n.becomeFollower(reply.Term)
		return
	}
	// Stale: either we've been deposed or the reply is for an older term.
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		n.nextIndex[peerID] = next + len(entries)
		n.matchIndex[peerID] = n.nextIndex[peerID] - 1
		n.tryAdvanceCommitLocked(term)
		return
	}

	// Failure: back off nextIndex using the conflict hint.
	if reply.ConflictTerm > 0 {
		// Find the last index in leader's log with that term.
		lastIdx := 0
		for i := len(n.log) - 1; i > 0; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				lastIdx = i
				break
			}
		}
		if lastIdx > 0 {
			n.nextIndex[peerID] = lastIdx + 1
		} else {
			n.nextIndex[peerID] = reply.ConflictIndex
		}
	} else {
		// Follower's log too short: skip to end of what follower has.
		if reply.ConflictIndex < 1 {
			n.nextIndex[peerID] = 1
		} else {
			n.nextIndex[peerID] = reply.ConflictIndex
		}
	}
	n.signalReplicate()
}

// tryAdvanceCommitLocked scans for N > commitIndex with a current-term entry
// replicated on a majority; if found, advances commitIndex and wakes applier.
// Runs with n.mu held.
func (n *Node) tryAdvanceCommitLocked(term int) {
	old := n.commitIndex
	total := len(n.peers) + 1
	majority := total/2 + 1
	for N := n.commitIndex + 1; N < len(n.log); N++ {
		if n.log[N].Term != term {
			continue // §5.4.2: never commit previous-term entries by counting
		}
		count := 1 // leader
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
		}
	}
	if n.commitIndex != old {
		log.Printf("[node %d] leader commitIndex %d -> %d", n.id, old, n.commitIndex)
		n.signalCommitReady()
	}
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Dead {
		return fmt.Errorf("node %d is dead", n.id)
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term < n.currentTerm {
		return nil
	}

	lastIdx := len(n.log) - 1
	lastTerm := n.log[lastIdx].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.resetElectionTimeout()
		n.savePersist()
		reply.VoteGranted = true
		log.Printf("[node %d] <-- granted vote to %d term=%d", n.id, args.CandidateID, args.Term)
	} else {
		log.Printf("[node %d] <-- denied vote to %d term=%d (votedFor=%d upToDate=%v)",
			n.id, args.CandidateID, args.Term, n.votedFor, upToDate)
	}
	return nil
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Dead {
		return fmt.Errorf("node %d is dead", n.id)
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply.Term = n.currentTerm
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0

	if args.Term < n.currentTerm {
		return nil
	}

	// Same-term AE from the legitimate leader: reset election timer, drop out
	// of Candidate if we were racing.
	n.role = Follower
	n.resetElectionTimeout()

	// Consistency check: log must contain PrevLogIndex with PrevLogTerm.
	if args.PrevLogIndex >= len(n.log) {
		reply.ConflictIndex = len(n.log)
		reply.ConflictTerm = 0
		return nil
	}
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = n.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 0 && n.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return nil
	}

	// Merge entries. Walk them in order:
	//   - if idx is beyond current log, append everything from here and stop
	//   - if idx exists and terms match, no-op (idempotent replay)
	//   - if idx exists and terms differ, truncate at idx, append from here, stop
	// This does NOT truncate a suffix just because an out-of-date AE lacks it,
	// which is what the paper demands (delayed duplicate must be a no-op).
	changed := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx >= len(n.log) {
			n.log = append(n.log, args.Entries[i:]...)
			changed = true
			break
		}
		if n.log[idx].Term != entry.Term {
			n.log = append(n.log[:idx], args.Entries[i:]...)
			changed = true
			break
		}
	}
	if changed {
		n.savePersist()
	}

	// Advance commitIndex per §5.3: min(leaderCommit, index of last new entry).
	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		lastNew := args.PrevLogIndex + len(args.Entries)
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			old := n.commitIndex
			n.commitIndex = newCommit
			log.Printf("[node %d] follower commitIndex %d -> %d", n.id, old, n.commitIndex)
			n.signalCommitReady()
		}
	}
	reply.Success = true
	return nil
}

func (n *Node) applierLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.commitReadyCh:
		}
		n.mu.Lock()
		if n.role == Dead {
			n.mu.Unlock()
			return
		}
		start := n.lastApplied + 1
		end := n.commitIndex
		if end >= start {
			for i := start; i <= end; i++ {
				n.applyEntryLocked(n.log[i])
			}
			n.lastApplied = end
		}
		n.mu.Unlock()
	}
}

// applyEntryLocked mutates the KV state machine. Runs with n.mu held.
func (n *Node) applyEntryLocked(e LogEntry) {
	switch e.Op {
	case OpPut:
		n.kv[e.Key] = e.Value
	case OpDelete:
		delete(n.kv, e.Key)
	case OpNoOp:
		// no-op, used only to force a current-term commit event
	}
}

// Submit appends a client command if this node is leader. Returns the log
// index+term the entry was assigned; callers should use WaitForCommit to
// confirm the entry actually reached quorum before treating it as durable.
func (n *Node) Submit(op Op, key, value string) (index int, term int, ok bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Op: op, Key: key, Value: value})
	index = len(n.log) - 1
	term = n.currentTerm
	n.savePersist()
	log.Printf("[node %d] Submit accepted: %s %s=%s at index=%d term=%d",
		n.id, op, key, value, index, term)
	n.mu.Unlock()
	n.signalReplicate()
	return index, term, true
}

// WaitForCommit blocks until (index, term) is on this node's committed log or
// the timeout elapses. Returns false if the entry got overwritten (deposed
// leader) or we timed out.
func (n *Node) WaitForCommit(index, term int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		n.mu.Lock()
		if index < len(n.log) && n.log[index].Term == term && n.commitIndex >= index {
			n.mu.Unlock()
			return true
		}
		// Overwritten by higher-term leader.
		if index < len(n.log) && n.log[index].Term != term && n.currentTerm > term {
			n.mu.Unlock()
			return false
		}
		n.mu.Unlock()
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// GetRole returns the current role (for demo / test drivers).
func (n *Node) GetRole() (Role, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm
}

// PrintState is observation only.
func (n *Node) PrintState() {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("[node %d] state: role=%s term=%d log=%d commitIndex=%d lastApplied=%d kv=%v",
		n.id, n.role, n.currentTerm, len(n.log)-1, n.commitIndex, n.lastApplied, n.kv)
}

func (n *Node) signalReplicate() {
	select {
	case n.triggerReplicateCh <- struct{}{}:
	default:
	}
}

func (n *Node) signalCommitReady() {
	select {
	case n.commitReadyCh <- struct{}{}:
	default:
	}
}

// callRPC dials + calls with independent timeouts so a dead peer never blocks
// caller goroutines. Returns true iff the call round-tripped.
func callRPC(addr, method string, args any, reply any) bool {
	conn, err := net.DialTimeout("tcp", addr, rpcDialTimeout)
	if err != nil {
		return false
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	call := client.Go(method, args, reply, nil)
	select {
	case <-call.Done:
		return call.Error == nil
	case <-time.After(rpcCallTimeout):
		return false
	}
}

func fatalf(format string, a ...any) {
	log.Printf(format, a...)
	os.Exit(1)
}
