package main

import "fmt"

func main() {
	store := NewStore()

	// Set a couple of keys.
	store.Set("foo", "bar")
	store.Set("baz", "qux")

	value, ok := store.Get("foo")
	fmt.Printf("Get(%q) = %q, found=%v\n", "foo", value, ok)

	// Overwrite an existing key.
	store.Set("foo", "updated")
	value, ok = store.Get("foo")
	fmt.Printf("Get(%q) = %q, found=%v (after overwrite)\n", "foo", value, ok)

	// Get a key that was never set.
	value, ok = store.Get("missing")
	fmt.Printf("Get(%q) = %q, found=%v\n", "missing", value, ok)

	// Delete a key, then confirm it's gone.
	store.Delete("baz")
	value, ok = store.Get("baz")
	fmt.Printf("Get(%q) = %q, found=%v (after delete)\n", "baz", value, ok)
}
