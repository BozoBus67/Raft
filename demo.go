package main

import (
	"log"
	"time"
)

func runDemo(nodes []*Node) {
	leader := waitForLeader(nodes, 3*time.Second)
	if leader == nil {
		log.Fatalf("no leader elected within timeout")
	}

	// A few client ops. WaitForCommit after each so the printed state reflects
	// what the algorithm actually agreed on, not what happened to arrive first.
	ops := []struct {
		op         Op
		key, value string
	}{
		{OpPut, "x", "1"},
		{OpPut, "y", "2"},
		{OpPut, "x", "99"},
		{OpDelete, "y", ""},
	}
	for _, o := range ops {
		log.Printf("=== client submits: %s %s=%s ===", o.op, o.key, o.value)
		idx, term, ok := leader.Submit(o.op, o.key, o.value)
		if !ok {
			log.Printf("  leader deposed mid-demo, finding new one")
			leader = waitForLeader(nodes, 3*time.Second)
			if leader == nil {
				log.Fatalf("lost leadership and no replacement elected")
			}
			idx, term, ok = leader.Submit(o.op, o.key, o.value)
			if !ok {
				log.Fatalf("  new leader also rejected submit; giving up")
			}
		}
		if !leader.WaitForCommit(idx, term, 2*time.Second) {
			log.Fatalf("  entry index=%d term=%d did not commit in time", idx, term)
		}
	}

	// Let followers catch up on the last leaderCommit so their printed KV
	// reflects the same committed prefix. Without this, followers lag by up
	// to one heartbeat (leaderCommit is piggybacked, not pushed immediately).
	waitForAppliedOnAll(nodes, leaderLogLen(leader)-1, 2*time.Second)

	log.Println("=== state after client demo ===")
	for _, n := range nodes {
		n.PrintState()
	}

	// Follower-rejection probe: pick any non-leader and submit; should refuse.
	for _, n := range nodes {
		if n != leader {
			log.Printf("=== client submits to follower node %d (should reject) ===", n.id)
			if _, _, ok := n.Submit(OpPut, "z", "99"); ok {
				log.Fatalf("follower node %d accepted Submit (bug)", n.id)
			}
			log.Printf("[node %d] Submit correctly rejected (not leader)", n.id)
			break
		}
	}

	// Leader-failure probe: kill the current leader, watch a new one take over,
	// submit through it, verify all live nodes commit.
	log.Printf("=== killing leader node %d to force re-election ===", leader.id)
	leader.Shutdown()
	survivors := livingNodes(nodes)

	newLeader := waitForLeader(survivors, 3*time.Second)
	if newLeader == nil {
		log.Fatalf("no new leader after killing old one")
	}
	log.Printf("=== new leader is node %d ===", newLeader.id)

	idx, term, ok := newLeader.Submit(OpPut, "w", "after-failover")
	if !ok {
		log.Fatalf("new leader %d rejected Submit", newLeader.id)
	}
	if !newLeader.WaitForCommit(idx, term, 2*time.Second) {
		log.Fatalf("post-failover entry did not commit")
	}
	waitForAppliedOnAll(survivors, leaderLogLen(newLeader)-1, 2*time.Second)

	log.Println("=== final state on surviving nodes ===")
	for _, n := range survivors {
		n.PrintState()
	}
}

func waitForLeader(nodes []*Node, timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			role, _ := n.GetRole()
			if role == Leader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func livingNodes(nodes []*Node) []*Node {
	out := make([]*Node, 0, len(nodes))
	for _, n := range nodes {
		role, _ := n.GetRole()
		if role != Dead {
			out = append(out, n)
		}
	}
	return out
}

func leaderLogLen(n *Node) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log)
}

// waitForAppliedOnAll blocks until every non-dead node has lastApplied >= idx.
// Best-effort: returns quietly if the timeout hits.
func waitForAppliedOnAll(nodes []*Node, idx int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, n := range nodes {
			n.mu.Lock()
			dead := n.role == Dead
			applied := n.lastApplied
			n.mu.Unlock()
			if dead {
				continue
			}
			if applied < idx {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
