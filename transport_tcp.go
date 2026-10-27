//go:build !js

package main

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

type tcpTransport struct {
	addr        string
	listener    net.Listener
	dispatcher  *rpc.Server
	wg          sync.WaitGroup
	dialTimeout time.Duration
	callTimeout time.Duration
}

func NewTCPTransport(addr string) *tcpTransport {
	return &tcpTransport{
		addr:        addr,
		dialTimeout: rpcDialTimeout,
		callTimeout: rpcCallTimeout,
	}
}

func (t *tcpTransport) Start(handler any) error {
	t.dispatcher = rpc.NewServer()
	if err := t.dispatcher.RegisterName("Node", handler); err != nil {
		return err
	}
	l, err := net.Listen("tcp", t.addr)
	if err != nil {
		return err
	}
	t.listener = l
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go t.dispatcher.ServeConn(conn)
		}
	}()
	return nil
}

func (t *tcpTransport) SendRequestVote(addr string, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return t.call(addr, "Node.RequestVote", &args, reply)
}

func (t *tcpTransport) SendAppendEntries(addr string, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return t.call(addr, "Node.AppendEntries", &args, reply)
}

// call dials + calls with independent timeouts so a dead peer never blocks
// caller goroutines. Returns true iff the call round-tripped.
func (t *tcpTransport) call(addr, method string, args any, reply any) bool {
	conn, err := net.DialTimeout("tcp", addr, t.dialTimeout)
	if err != nil {
		return false
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	c := client.Go(method, args, reply, nil)
	select {
	case <-c.Done:
		return c.Error == nil
	case <-time.After(t.callTimeout):
		return false
	}
}

func (t *tcpTransport) Close() error {
	if t.listener != nil {
		_ = t.listener.Close()
	}
	t.wg.Wait()
	return nil
}
