//go:build js && wasm

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"syscall/js"
	"time"
)

// cluster is the WASM-side cluster manager. It owns every live Node and the
// address<->ID map used to update peer sets on add/delete. One instance per
// page, held by the global `world`.
type cluster struct {
	mu       sync.Mutex
	nodes    []*Node
	addrByID map[int]string
	ticks    int // monotonic snapshot counter; incremented every tick() call
	rngSeed  int64
}

var world *cluster

func main() {
	world = newCluster()
	world.boot()

	js.Global().Set("raftGetState", js.FuncOf(jsGetState))
	js.Global().Set("raftTick", js.FuncOf(jsTick))
	js.Global().Set("raftSubmit", js.FuncOf(jsSubmit))
	js.Global().Set("raftAddNode", js.FuncOf(jsAddNode))
	js.Global().Set("raftDeleteNode", js.FuncOf(jsDeleteNode))
	js.Global().Set("raftReset", js.FuncOf(jsReset))

	// Signal readiness so the JS loader stops polling.
	js.Global().Set("raftReady", true)

	// Keep the Go runtime alive. main() returning stops the whole wasm module,
	// including the goroutines that drive the election timers.
	select {}
}

func newCluster() *cluster {
	return &cluster{
		addrByID: map[int]string{},
		rngSeed:  time.Now().UnixNano(),
	}
}

func (c *cluster) boot() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startFresh(3)
}

// startFresh assumes c.mu is held and (c.nodes already shut down OR empty).
func (c *cluster) startFresh(n int) {
	c.nodes = nil
	c.addrByID = map[int]string{}
	c.ticks = 0
	for i := 1; i <= n; i++ {
		c.addrByID[i] = fmt.Sprintf("mem:%d", i)
	}
	for i := 1; i <= n; i++ {
		c.addNodeLocked(i)
	}
}

// addNodeLocked constructs a node with id, giving it every other known addr as
// a peer. Assumes c.mu is held and c.addrByID already contains id.
func (c *cluster) addNodeLocked(id int) *Node {
	peers := map[int]string{}
	for pid, addr := range c.addrByID {
		if pid != id {
			peers[pid] = addr
		}
	}
	transport := NewMemTransport(c.addrByID[id])
	storage := NewMemStorage()
	rng := rand.New(rand.NewSource(c.rngSeed ^ int64(id)*2654435761))
	n := NewNode(id, peers, transport, NewRealClock(), storage, rng)
	if err := n.Serve(); err != nil {
		return nil
	}
	c.nodes = append(c.nodes, n)
	return n
}

// ---------- JS bindings ----------

func jsGetState(this js.Value, args []js.Value) any {
	c := world
	c.mu.Lock()
	nodes := make([]any, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, snapshotNode(n))
	}
	tick := c.ticks
	c.mu.Unlock()
	return map[string]any{
		"nodes": nodes,
		"tick":  tick,
	}
}

func jsTick(this js.Value, args []js.Value) any {
	world.mu.Lock()
	world.ticks++
	world.mu.Unlock()
	return nil
}

func jsSubmit(this js.Value, args []js.Value) any {
	if len(args) < 3 {
		return false
	}
	op := Op(args[0].String())
	key := args[1].String()
	value := args[2].String()

	world.mu.Lock()
	nodes := make([]*Node, len(world.nodes))
	copy(nodes, world.nodes)
	world.mu.Unlock()

	for _, n := range nodes {
		role, _ := n.GetRole()
		if role != Leader {
			continue
		}
		_, _, ok := n.Submit(op, key, value)
		return ok
	}
	return false
}

func jsAddNode(this js.Value, args []js.Value) any {
	world.mu.Lock()
	defer world.mu.Unlock()

	newID := 1
	for id := range world.addrByID {
		if id >= newID {
			newID = id + 1
		}
	}
	addr := fmt.Sprintf("mem:%d", newID)
	world.addrByID[newID] = addr

	// Add this new peer to every existing node's peer map first so they
	// start replicating to it as soon as it comes up.
	for _, existing := range world.nodes {
		existing.mu.Lock()
		existing.peers[newID] = addr
		if existing.role == Leader {
			existing.nextIndex[newID] = len(existing.log)
			existing.matchIndex[newID] = 0
		}
		existing.mu.Unlock()
	}

	c := world
	c.addNodeLocked(newID)
	return nil
}

func jsDeleteNode(this js.Value, args []js.Value) any {
	if len(args) < 1 {
		return nil
	}
	id := args[0].Int()

	world.mu.Lock()

	var target *Node
	kept := world.nodes[:0]
	for _, n := range world.nodes {
		if n.id == id {
			target = n
			continue
		}
		kept = append(kept, n)
	}
	world.nodes = kept
	delete(world.addrByID, id)

	// Drop the dead peer from every survivor's peer map.
	for _, n := range world.nodes {
		n.mu.Lock()
		delete(n.peers, id)
		delete(n.nextIndex, id)
		delete(n.matchIndex, id)
		n.mu.Unlock()
	}

	world.mu.Unlock()

	// Shutdown last so its ongoing goroutines don't contest other nodes'
	// locks while we mutate c.nodes.
	if target != nil {
		target.Shutdown()
	}
	return nil
}

func jsReset(this js.Value, args []js.Value) any {
	world.mu.Lock()
	old := world.nodes
	world.nodes = nil
	world.mu.Unlock()

	for _, n := range old {
		n.Shutdown()
	}

	// Clear the registry so fresh addr→handler mappings win.
	regMu.Lock()
	registry = map[string]nodeHandler{}
	regMu.Unlock()

	world.mu.Lock()
	world.startFresh(3)
	world.mu.Unlock()
	return nil
}

// ---------- state → JS shape ----------

func snapshotNode(n *Node) map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()

	entries := make([]any, 0, len(n.log))
	for i, e := range n.log {
		op := string(e.Op)
		if i == 0 && op == "" {
			op = "sentinel"
		}
		item := map[string]any{
			"term": e.Term,
			"op":   op,
		}
		if e.Key != "" {
			item["key"] = e.Key
		}
		if e.Value != "" {
			item["value"] = e.Value
		}
		entries = append(entries, item)
	}

	kv := map[string]any{}
	for k, v := range n.kv {
		kv[k] = v
	}

	return map[string]any{
		"id":          n.id,
		"role":        n.role.String(),
		"term":        n.currentTerm,
		"log":         entries,
		"commitIndex": n.commitIndex,
		"lastApplied": n.lastApplied,
		"kv":          kv,
	}
}
