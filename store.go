package main

import "sync"

// Store uses a sync.RWMutex rather than a plain sync.Mutex because reads
// (Get) are expected to happen far more often than writes (Set/Delete) in a
// key-value store's workload. RWMutex lets any number of readers hold the
// lock at the same time, so concurrent Gets don't block each other; it only
// forces exclusive access when a writer needs to change the map.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore creates an empty, ready-to-use Store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Get returns the value stored for key and whether it was found.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

// Set stores value under key, overwriting any existing value.
func (s *Store) Set(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// Delete removes key from the store, if present.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}
