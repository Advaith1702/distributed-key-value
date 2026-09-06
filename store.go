package main

import (
	"container/list"
	"sync"
	"time"
)

// reapInterval is how often the background reaper sweeps the store for
// expired keys. It only bounds how long a dead key keeps occupying memory
// and LRU capacity, not how long it stays visible: Get checks expiry itself,
// so a key is unreadable the instant it expires regardless of this.
const reapInterval = time.Second

// entry is what's stored in each list.Element.Value. It carries the key
// alongside the value so that when a node is evicted from the back of the
// list, we know which key to remove from items — the list node itself has
// no idea what map key points to it.
type entry struct {
	key   string
	value string

	// expiresAt is the instant this entry stops being visible. The zero
	// Time means "no expiration", which is what a Set with ttl == 0
	// stores. An absolute deadline is kept rather than the original
	// duration so that expiry does not depend on when anyone happens to
	// look.
	expiresAt time.Time
}

// expired reports whether this entry's deadline has passed. An entry with
// no deadline never expires.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
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

// NewStore creates an empty Store that holds at most capacity keys, and
// starts the background reaper that clears expired keys out of it.
// capacity must be >= 1.
func NewStore(capacity int) *Store {
	s := &Store{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		ll:       list.New(),
	}

	go s.reapLoop()

	return s
}

// reapLoop wakes up every reapInterval and clears out expired keys. It runs
// for the lifetime of the process; the Store lives as long as the node
// does, so there is nothing to shut it down for.
func (s *Store) reapLoop() {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.reapExpired()
	}
}

// reapExpired removes every entry whose deadline has passed, from both the
// map and the recency list.
//
// This runs on its own goroutine, which is exactly why it takes s.mu like
// every other method here. The race it prevents is concrete: the reaper
// walks s.items deleting keys while a client goroutine is in Get or Set
// touching that same map. Go maps are not safe for concurrent use with any
// writer present — a client reading a key while the reaper deletes one is
// not a stale-value problem, it is a runtime crash ("concurrent map read
// and map write") or silent corruption of the map's internal buckets.
//
// The list is just as exposed. A key lives in two places at once, so
// removing one means mutating both: if the reaper called ll.Remove(elem)
// while a client's Get was calling ll.MoveToFront(elem) on that same
// element, the two would be rewriting the same next/prev pointers with no
// ordering between them, and the list could end up with a node that is
// unreachable, doubly linked to itself, or pointing at a node already
// removed. Holding the one mutex for the whole sweep makes each pass
// atomic with respect to every client operation: a client sees the store
// either entirely before or entirely after the sweep, never midway
// through it.
func (s *Store) reapExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	// Deleting from a map while ranging over it is explicitly allowed in
	// Go: entries removed during the loop are simply not produced later.
	for key, elem := range s.items {
		if elem.Value.(entry).expired(now) {
			s.ll.Remove(elem)
			delete(s.items, key)
		}
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

	// Expiry is checked here rather than left to the reaper. The reaper
	// only runs once a second, so between a key's deadline and the next
	// sweep it is still sitting in the map — returning it would hand back
	// a value the caller was promised had already gone. Dropping it here
	// makes the deadline exact from a reader's point of view, and the
	// reaper's job is reduced to reclaiming space for keys nobody asks
	// for again.
	if elem.Value.(entry).expired(time.Now()) {
		s.ll.Remove(elem)
		delete(s.items, key)
		return "", false
	}

	s.ll.MoveToFront(elem)
	return elem.Value.(entry).value, true
}

// Set stores value under key, overwriting any existing value. A ttl above
// zero makes the key expire that far in the future; a ttl of zero means it
// never expires. Overwriting a key replaces its expiration too, so a Set
// with no ttl clears an expiration an earlier Set put there.
//
// If key is new and the store is already at capacity, the
// least-recently-used key is evicted first to make room.
func (s *Store) Set(key string, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	if elem, ok := s.items[key]; ok {
		elem.Value = entry{key: key, value: value, expiresAt: expiresAt}
		s.ll.MoveToFront(elem)
		return
	}

	if len(s.items) >= s.capacity {
		// Note that this evicts the least recently used key even if some
		// other key in the store has already expired but not yet been
		// reaped. Checking for an expired victim first would be a
		// reasonable improvement; it is left out to keep eviction the
		// same O(1) operation it has been since Milestone 2.
		lru := s.ll.Back()
		s.ll.Remove(lru)
		delete(s.items, lru.Value.(entry).key)
	}

	elem := s.ll.PushFront(entry{key: key, value: value, expiresAt: expiresAt})
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
