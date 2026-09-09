package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type PersistedState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// wipeStateDir clears any prior run's persisted state.
// Called ONCE from main before nodes are constructed; NOT from NewNode so
// intra-run node restarts still recover their own log.
func wipeStateDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0755)
}

func stateFilePath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("node-%d.json", id))
}

// savePersist writes currentTerm+votedFor+log atomically: write to .tmp, fsync
// the file, close, rename over the real path. Rename on POSIX is atomic.
// Called with n.mu held.
func (n *Node) savePersist() {
	state := PersistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}
	data, err := json.Marshal(state)
	if err != nil {
		fatalf("[node %d] persist marshal: %v", n.id, err)
	}
	tmp := n.storagePath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fatalf("[node %d] persist open: %v", n.id, err)
	}
	if _, err := f.Write(data); err != nil {
		fatalf("[node %d] persist write: %v", n.id, err)
	}
	if err := f.Sync(); err != nil {
		fatalf("[node %d] persist fsync: %v", n.id, err)
	}
	if err := f.Close(); err != nil {
		fatalf("[node %d] persist close: %v", n.id, err)
	}
	if err := os.Rename(tmp, n.storagePath); err != nil {
		fatalf("[node %d] persist rename: %v", n.id, err)
	}
}

// loadPersist reads currentTerm+votedFor+log. If the file is absent this is a
// fresh boot and the caller keeps the sentinel-only log it already installed.
// Called with n.mu held.
func (n *Node) loadPersist() {
	data, err := os.ReadFile(n.storagePath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		fatalf("[node %d] persist read: %v", n.id, err)
	}
	var state PersistedState
	if err := json.Unmarshal(data, &state); err != nil {
		fatalf("[node %d] persist unmarshal: %v", n.id, err)
	}
	if len(state.Log) == 0 || state.Log[0].Term != 0 {
		fatalf("[node %d] persist: malformed log (missing sentinel)", n.id)
	}
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = state.Log
}
