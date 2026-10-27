//go:build !js

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type fileStorage struct {
	path string
}

func NewFileStorage(dir string, id int) *fileStorage {
	return &fileStorage{path: filepath.Join(dir, fmt.Sprintf("node-%d.json", id))}
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

// Save writes currentTerm+votedFor+log atomically: write to .tmp, fsync the
// file, close, rename over the real path. Rename on POSIX is atomic.
func (s *fileStorage) Save(state PersistedState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// Load reads currentTerm+votedFor+log. If the file is absent, found=false and
// the caller keeps whatever sentinel-only log it already installed.
func (s *fileStorage) Load() (PersistedState, bool, error) {
	var state PersistedState
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, false, nil
		}
		return state, false, err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, false, err
	}
	if len(state.Log) == 0 || state.Log[0].Term != 0 {
		return state, false, fmt.Errorf("malformed log (missing sentinel)")
	}
	return state, true, nil
}
