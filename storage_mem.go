//go:build js && wasm

package main

import "sync"

type memStorage struct {
	mu    sync.Mutex
	state PersistedState
	found bool
}

func NewMemStorage() *memStorage { return &memStorage{} }

func (s *memStorage) Save(state PersistedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.found = true
	return nil
}

func (s *memStorage) Load() (PersistedState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.found, nil
}
