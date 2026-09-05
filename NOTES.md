# Learning Notes

A running explanation of what the code does and why, written after each
milestone. Read top to bottom for a build-up of the whole project; each
section only assumes what came before it.

## Milestone 1 — Single-node in-memory store

**Files:** [go.mod](go.mod), [store.go](store.go), [main.go](main.go)

### What a Go module is

`go.mod` is the file that turns this directory into a Go module — a unit of
code with a name (`kvstore`) that other packages could import, and a
minimum Go version it requires. `go mod init kvstore` created it. You don't
need to touch it by hand; the `go` toolchain manages it (it just bumped the
`go` line to `1.27.1` on its own, to record the toolchain version it built
with).

### The `Store` struct

```go
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}
```

This is a plain Go struct with two fields: the actual data (`data`, a
`map[string]string`), and a lock (`mu`) that protects it. Nothing enforces
that you use `mu` correctly — Go doesn't tie a mutex to the data it guards
the way some languages do. The convention (used here) is: never touch
`data` without holding `mu` first, and only touch it inside the `Store`'s
own methods so that rule is easy to audit.

**Why a mutex at all?** Go maps are not safe for concurrent use. If one
goroutine writes to a map while another reads or writes it at the same
time, the program can corrupt its own memory or crash outright (Go's
runtime actively detects and panics on some of these cases: "concurrent
map writes"). Since this store will eventually be hit from multiple
goroutines at once (multiple client connections in Milestone 3, replication
in Milestone 4), it needs to be safe from the start.

**Why `RWMutex` instead of plain `Mutex`?** A `sync.Mutex` has one mode:
locked or unlocked, one owner at a time, full stop — a `Get` from one
goroutine would block a `Get` from another goroutine even though neither
is modifying anything. A `sync.RWMutex` distinguishes two kinds of lock:

- `RLock()` / `RUnlock()` — a "read lock." Any number of goroutines can
  hold a read lock at the same time.
- `Lock()` / `Unlock()` — a "write lock." Exclusive: while one goroutine
  holds it, no one else (reader or writer) can hold any lock.

A key-value store is a read-heavy workload — far more `Get`s than
`Set`/`Delete`s in most real usage — so letting reads run in parallel
instead of serializing them behind a single mutex is a meaningful win, at
the cost of a slightly more complex API (two lock methods instead of one).
This tradeoff is called out directly in the comment above the struct in
[store.go](store.go), because it's the kind of design decision that isn't
obvious just from reading the method bodies.

### The constructor: `NewStore()`

```go
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}
```

Go's zero value for a map is `nil`, and reading from a `nil` map returns
the zero value/`false` (safe), but *writing* to a nil map panics. So the
struct can't just be created with `Store{}` and used immediately — the map
field needs to be initialized with `make` first. `NewStore` exists so
callers never have to remember that; they just call `NewStore()` and get a
struct that's always ready to use. This "constructor function" pattern is
idiomatic Go — the language has no real constructors, so a `NewX` function
returning `*X` is the conventional stand-in.

The zero value of `sync.RWMutex` (and `sync.Mutex`) *is* ready to use
as-is — that's why `mu` doesn't need any initialization in `NewStore`.

### The three methods

```go
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}
```

- **Pointer receiver (`s *Store`)**: all three methods take `*Store`, not
  `Store`. This matters here for two reasons: (1) the struct contains a
  mutex, and copying a mutex (which a value receiver would do on every
  call) is a bug — Go even has a vet check for it; (2) `Set`/`Delete` need
  to mutate the shared map, which requires a pointer.
- **`defer s.mu.RUnlock()`**: `defer` schedules the unlock to run when the
  function returns, no matter how it returns. With only one return
  statement here it's not strictly load-bearing yet, but it's the standard
  idiom because it keeps the "lock, do work, unlock" pattern in one place
  right next to the lock call — you can't add a new early return later and
  accidentally forget to unlock.
- **The comma-ok idiom (`value, ok := s.data[key]`)**: indexing a Go map
  with one return value gives you the zero value (`""` for a string) when
  the key is missing, with no way to distinguish "missing" from
  "explicitly set to empty string." Asking for two return values gives you
  that second `ok bool` to disambiguate. `Get` just passes this straight
  through to its own caller, which is why its signature is
  `(string, bool)` too.

`Set` and `Delete` follow the same shape but call `s.mu.Lock()` /
`s.mu.Unlock()` (the exclusive write lock) instead of the read lock, since
they mutate `data`.

### `main.go`: manual verification

`main.go` isn't part of the store's design — it's a throwaway harness to
*see* the store work: create one, `Set` a couple of keys, `Get` one back,
overwrite a key, look up a missing key, `Delete` a key and confirm it's
gone afterward. Each step prints its result with `fmt.Printf` so running
`go run .` gives an immediate, readable trace of correct behavior. This
file will very likely be replaced by a real client/server interaction once
Milestone 3 adds a TCP protocol — it's scaffolding for this milestone only.

### Verified

```
go build ./...   # compiles cleanly
go vet ./...     # no suspicious constructs (e.g. no mutex-copying)
go run .         # prints the expected Get/Set/Delete trace
```
