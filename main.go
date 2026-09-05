package main

import "fmt"

func main() {
	store := NewStore(3)

	// Fill the store to capacity.
	store.Set("a", "1")
	store.Set("b", "2")
	store.Set("c", "3")

	// Touch "a" so it becomes the most-recently-used key, leaving "b" as
	// the least-recently-used one (it hasn't been Get/Set since it was
	// added, unlike "a" and "c").
	store.Get("a")

	// The store is at capacity, so this Set must evict the
	// least-recently-used key: "b", not "a" (even though "a" was set
	// before "b") because "a" was just touched above.
	store.Set("d", "4")

	for _, key := range []string{"a", "b", "c", "d"} {
		value, ok := store.Get(key)
		fmt.Printf("Get(%q) = %q, found=%v\n", key, value, ok)
	}
}
