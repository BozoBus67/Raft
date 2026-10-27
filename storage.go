package main

type PersistedState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

type Storage interface {
	Save(state PersistedState) error
	Load() (state PersistedState, found bool, err error)
}
