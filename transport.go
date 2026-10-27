package main

type Transport interface {
	Start(handler any) error
	SendRequestVote(addr string, args RequestVoteArgs, reply *RequestVoteReply) bool
	SendAppendEntries(addr string, args AppendEntriesArgs, reply *AppendEntriesReply) bool
	Close() error
}
