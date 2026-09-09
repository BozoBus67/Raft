package main

import (
	"log"
	"math/rand"
	"time"
)

type NodeConfig struct {
	ID   int
	Addr string
}

var clusterConfig = []NodeConfig{
	{ID: 1, Addr: "127.0.0.1:9201"},
	{ID: 2, Addr: "127.0.0.1:9202"},
	{ID: 3, Addr: "127.0.0.1:9203"},
}

const stateDir = "state"

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())

	if err := wipeStateDir(stateDir); err != nil {
		log.Fatalf("wipe state dir: %v", err)
	}

	nodes := makeCluster()
	startServers(nodes)
	time.Sleep(200 * time.Millisecond) // let listeners bind

	runDemo(nodes)

	shutdownAll(nodes)
}

func makeCluster() []*Node {
	// Every node sees every OTHER node's address.
	addrByID := map[int]string{}
	for _, c := range clusterConfig {
		addrByID[c.ID] = c.Addr
	}
	nodes := make([]*Node, 0, len(clusterConfig))
	for _, c := range clusterConfig {
		peers := map[int]string{}
		for id, addr := range addrByID {
			if id != c.ID {
				peers[id] = addr
			}
		}
		nodes = append(nodes, NewNode(c.ID, c.Addr, peers, stateDir))
	}
	return nodes
}

func startServers(nodes []*Node) {
	for _, n := range nodes {
		if err := n.Serve(); err != nil {
			log.Fatalf("node %d serve: %v", n.id, err)
		}
	}
}

func shutdownAll(nodes []*Node) {
	for _, n := range nodes {
		n.Shutdown()
	}
}
