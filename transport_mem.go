//go:build js && wasm

package main

import "sync"

// In-browser cluster — all nodes are in the same process, so RPCs are direct
// method calls on the target node, not real network sends. A shared registry
// maps peer addresses to the handlers that received them in Start().

type nodeHandler interface {
	RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error
	AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error
}

var (
	regMu    sync.RWMutex
	registry = map[string]nodeHandler{}
)

type memTransport struct {
	addr string
}

func NewMemTransport(addr string) *memTransport {
	return &memTransport{addr: addr}
}

func (t *memTransport) Start(handler any) error {
	h, ok := handler.(nodeHandler)
	if !ok {
		return nil
	}
	regMu.Lock()
	registry[t.addr] = h
	regMu.Unlock()
	return nil
}

func (t *memTransport) SendRequestVote(addr string, args RequestVoteArgs, reply *RequestVoteReply) bool {
	regMu.RLock()
	h, ok := registry[addr]
	regMu.RUnlock()
	if !ok {
		return false
	}
	return h.RequestVote(args, reply) == nil
}

func (t *memTransport) SendAppendEntries(addr string, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	regMu.RLock()
	h, ok := registry[addr]
	regMu.RUnlock()
	if !ok {
		return false
	}
	return h.AppendEntries(args, reply) == nil
}

func (t *memTransport) Close() error {
	regMu.Lock()
	delete(registry, t.addr)
	regMu.Unlock()
	return nil
}
