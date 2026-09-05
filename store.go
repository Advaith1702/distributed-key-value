package main

import (
	"container/list"
	"sync"
)

// entry is what's stored in each list.Element.Value. It carries the key
// alongside the value so that when a node is evicted from the back of the
// list, we know which key to remove from items — the list node itself has
// no idea what map key points to it.
type entry struct {
	key   string
	value string
}

// Store uses a plain sync.Mutex, not a sync.RWMutex. In the Milestone 1
// version (no eviction), RWMutex made sense because Get was a pure read.
// With LRU eviction, Get has to move the touched node to the front of the
// recency list on every call — so it mutates shared state just like Set
// and Delete do. There's no longer a read-only path to give a separate,
// shared RLock, so a plain Mutex is the honest choice here.
type Store struct {
	mu       sync.Mutex
	capacity int

	// items maps key -> its node in ll, giving O(1) lookup by key. A map
	// alone can't implement LRU on its own: Go maps have no ordering, so
	// there's no way to ask "which key was used longest ago" from the map
	// alone.
	items map[string]*list.Element

	// ll holds the same entries in recency order: front = most recently
	// used, back = least recently used. A list alone can't implement LRU
	// either: finding a given key's node would require scanning the list
	// end to end (O(n)) — the map above is what makes that lookup O(1).
	//
	// This uses the standard library's container/list rather than a
	// hand-rolled doubly linked list. container/list already gives us
	// O(1) PushFront/MoveToFront/Remove/Back, and reimplementing that
	// pointer-juggling here wouldn't teach anything new about LRU itself.
	ll *list.List
}

// NewStore creates an empty Store that holds at most capacity keys.
// capacity must be >= 1.
func NewStore(capacity int) *Store {
	return &Store{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		ll:       list.New(),
	}
}

// Get returns the value stored for key and whether it was found. A
// successful Get counts as "using" the key, so it moves that key's node
// to the front of the recency list.
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, ok := s.items[key]
	if !ok {
		return "", false
	}

	s.ll.MoveToFront(elem)
	return elem.Value.(entry).value, true
}

// Set stores value under key, overwriting any existing value. If key is
// new and the store is already at capacity, the least-recently-used key
// is evicted first to make room.
func (s *Store) Set(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.items[key]; ok {
		elem.Value = entry{key: key, value: value}
		s.ll.MoveToFront(elem)
		return
	}

	if len(s.items) >= s.capacity {
		lru := s.ll.Back()
		s.ll.Remove(lru)
		delete(s.items, lru.Value.(entry).key)
	}

	elem := s.ll.PushFront(entry{key: key, value: value})
	s.items[key] = elem
}

// Delete removes key from the store, if present.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, ok := s.items[key]
	if !ok {
		return
	}

	s.ll.Remove(elem)
	delete(s.items, key)
}
