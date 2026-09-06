# Learning Notes

A running explanation of what the code does and why, written after each
milestone. Read top to bottom for a build-up of the whole project; each
section only assumes what came before it.

Each section describes the code as it stood at the end of that stage, so an
earlier one can describe flags or signatures that a later one replaces --
Milestone 5 drops `-port`, `-role` and `-followers` for `-addr` and
`-cluster`, and Milestone 6 adds a `ttl` parameter to `Set`. That is the
point of reading it in order: the reasoning for a change is in the section
that makes it.

For what the project does *now*, read [README.md](README.md). For how it got
there, read on.

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

## Milestone 2 — LRU eviction

**Files:** [store.go](store.go), [main.go](main.go)

### The problem: bounded capacity needs an eviction policy

Milestone 1's store could grow forever — every `Set` just added another
map entry. A real cache needs a capacity limit, and once you hit it, you
need a rule for *which* key to throw out to make room for a new one.
"Least-recently-used" (LRU) is a common rule: throw out whichever key
hasn't been touched (via `Get` or `Set`) in the longest time, on the
theory that keys used recently are more likely to be used again soon.

### Why one data structure isn't enough

LRU needs two things at once, and no single structure gives you both
cheaply:

- **Fast lookup by key** — a `map[string]V]` gives O(1) lookup, but Go
  maps have no ordering. Iterating one to find "the key used longest ago"
  would be an unordered mess with no notion of "longest ago" at all.
- **Recency order** — a linked list ordered from most- to
  least-recently-used gives you O(1) "who's least recently used?" (just
  look at the back) and O(1) "move this to the front" (if you already
  have a pointer to its node). But finding *which* node belongs to a
  given key, starting from just the key, would mean scanning the whole
  list — O(n).

The classic fix, used here, is to keep both structures at once and have
them point into each other:

```go
items map[string]*list.Element   // key -> node, for O(1) lookup
ll    *list.List                 // recency order, for O(1) reordering/eviction
```

`items` gets you from a key straight to its list node in O(1). `ll` keeps
the nodes ordered by recency, front = most-recently-used, back =
least-recently-used, so eviction is always "remove `ll.Back()`." Neither
structure could do this alone; together they give O(1) for every LRU
operation. This is why the field comments in [store.go](store.go) spell
out specifically what each structure *can't* do by itself.

### Why `container/list` instead of a hand-rolled linked list

Go's standard library ships a doubly linked list (`container/list`) with
`PushFront`, `MoveToFront`, `Back`, and `Remove`, all O(1). Writing a
linked list by hand (raw `next`/`prev` pointers, manually patching
neighbors on removal) is a good exercise once, but it doesn't teach
anything new about *LRU* specifically — the interesting part of this
milestone is the two-structure design, not pointer-juggling. So this
implementation uses the standard library one, with a comment at the
`ll` field saying so.

### The `entry` struct

```go
type entry struct {
	key   string
	value string
}
```

Each `list.Element.Value` holds an `entry`, not just the value string.
That's because eviction starts from the *list* side: `s.ll.Back()` gives
you a node, but a node on its own doesn't know which map key points to
it. Stashing the key inside the node's value is what lets eviction do
`delete(s.items, lru.Value.(entry).key)` — without it, there'd be no way
to remove the evicted key from `items`.

### Walking through the methods

**`Get`** now does two things instead of one: look up the value, *and*
mark the key as just-used by calling `s.ll.MoveToFront(elem)`. That
second part is what makes it LRU instead of a static cache — every touch
resets the clock on that key.

**`Set`** has three cases:
1. Key already exists → update its `entry.value` in place and
   `MoveToFront` it (using it counts as touching it, same as `Get`).
2. Key is new and the store is full (`len(s.items) >= s.capacity`) →
   evict `s.ll.Back()` first (remove from both `ll` and `items`), *then*
   insert.
3. Key is new and there's room → just insert (`PushFront` + record in
   `items`).

**`Delete`** is mostly unchanged in shape, but now has to clean up both
structures — `s.ll.Remove(elem)` as well as `delete(s.items, key)` —
since leaving a stale node in `ll` after removing it from `items` would
leak memory and corrupt the recency order.

### Why `sync.RWMutex` became `sync.Mutex`

This is the most important design change in this milestone, and it's a
direct reversal of something explained in Milestone 1. Back then, `Get`
was a pure read — it only looked at the map, never changed anything — so
`RWMutex` let multiple `Get`s run concurrently via `RLock`.

With LRU, `Get` calls `s.ll.MoveToFront(elem)`, which *mutates* the
recency list. Two goroutines calling `Get` at the same time would both be
writing to `ll` concurrently — exactly the unsafe scenario a lock exists
to prevent. So `Get` can no longer safely use `RLock`; it needs the same
exclusive `Lock` that `Set` and `Delete` use. Once every method needs the
exclusive lock, `RWMutex`'s whole reason for existing (letting reads run
in parallel) disappears — it would still *work* correctly, but `RLock`
would simply never be called anywhere, which is misleading dead
capability to leave sitting in the type. Switching to a plain
`sync.Mutex` makes the code honestly reflect that every operation here is
now a write, matching the "clarity over cleverness" priority for this
project.

The lesson: a locking strategy is a property of *what the code does to
its data*, not just the type signature (`Get` "sounds" read-only, but
its implementation isn't anymore) — it's worth re-checking after any
change that alters what a method touches.

### `main.go`: demonstrating recency, not just capacity

The demo sets 3 keys at capacity 3 (`a`, `b`, `c`), then calls `Get("a")`
before inserting a 4th key (`d`). If eviction were plain FIFO
(oldest-inserted-first), `a` would be evicted. Because it's LRU and `a`
was just touched, `b` is evicted instead — proving the recency tracking
actually works, not just that *some* key gets dropped at capacity.

### Verified

```
go build ./...   # compiles cleanly
go vet ./...     # no suspicious constructs
go run .         # confirms "b" evicted, "a"/"c"/"d" still present
```

## Milestone 3 — TCP server

**Files:** [server.go](server.go), [main.go](main.go)

### From a demo to a real server

Milestones 1-2 were verified by a `main.go` that called `Store` methods
directly and printed the results — useful for building the store, but not
how a real key-value store gets used: over the network, from a separate
client process. `main.go` now does something completely different: it
starts a TCP server and never returns, so `store.go`'s `Get`/`Set`/`Delete`
are called only in response to bytes arriving over a socket, in
[server.go](server.go).

Note what *didn't* need to change: `store.go` is untouched. Its mutex was
already built in Milestone 1 to make `Store` safe for concurrent callers
— it didn't know or care whether those callers would be goroutines racing
in a unit test or goroutines racing because of real network connections.
That's the payoff of getting the concurrency-safety right early.

### The protocol: one line in, one line out

The wire format is deliberately simple: a client sends a line of text
ending in `\n`, the server sends back a line of text ending in `\n`, and
that repeats until the client disconnects. This maps naturally onto
`bufio.Scanner`, whose default behavior (`bufio.ScanLines`) is exactly
"give me one line at a time, with the trailing newline (and any `\r`)
already stripped." Without `bufio.Scanner`, reading a `net.Conn` directly
would mean manually accumulating bytes into a buffer until a `\n` shows
up — `Scanner` exists so nobody has to hand-roll that.

### `ListenAndServe` and the Accept loop

```go
ln, err := net.Listen("tcp", addr)
...
for {
	conn, err := ln.Accept()
	...
	go handleConnection(conn, store)
}
```

`net.Listen` opens the TCP port and starts queuing incoming connection
attempts, but doesn't hand you a connection yet — `ln.Accept()` blocks
until one is ready, then returns a `net.Conn` for that specific client.
Critically, `Accept()` only ever gives you *one* connection per call; to
serve more than one client, you have to call it again.

**Why `go handleConnection(conn, store)` instead of just calling
`handleConnection(conn, store)` directly?** If the loop body handled the
connection itself — reading commands, writing responses, all
synchronously — the loop couldn't call `Accept()` again until that client
disconnected. A single client that connects and then sits idle (or is
just slow) would block every other client from ever connecting, since the
server would be stuck serving one connection at a time. Spawning a
goroutine per connection means the loop's only job is "accept, launch a
goroutine, immediately go around and accept the next one" — so however
many clients connect, each gets its own independent, concurrently-running
handler, and Go's scheduler multiplexes all those goroutines onto the
OS's threads. This is the standard shape for TCP servers in Go; the
`net/http` package does the same thing internally per-request.

This is also *why* `store.go`'s mutex is load-bearing here for the first
time in a way that's easy to see: with N clients connected, there can be
N goroutines calling `store.Get`/`Set`/`Delete` at literally the same
moment. The mutex is what prevents that from corrupting the map or the
LRU list.

On an `Accept()` error, the loop logs it and `continue`s rather than
returning/crashing — one failed accept (e.g. a transient resource limit)
isn't a reason to bring down every already-connected client. A real
production server would also distinguish "transient, retry with backoff"
errors from "fatal, the listener itself is broken" errors; this
implementation doesn't make that distinction, since it's outside this
milestone's scope.

### `handleConnection` and `handleCommand`

`handleConnection` is intentionally thin: `defer conn.Close()` (so the
socket always gets cleaned up when the client disconnects or an error
ends the scan loop), then a loop that reads one line, hands it to
`handleCommand`, and writes back whatever string comes out. All of the
actual protocol logic — parsing, deciding what's valid, calling the
store — lives in `handleCommand(line string, store *Store) string`, kept
as a plain function that takes a string and returns a string with no
network types involved. That separation means the protocol logic can be
reasoned about (or unit tested, later) without needing a real socket at
all.

Inside `handleCommand`:

- **`strings.Fields(line)`** splits on any whitespace and discards empty
  results, so `"GET   foo"` (extra spaces) still parses to `["GET",
  "foo"]`. This is used for `GET` and `DELETE`, which each expect exactly
  one argument.
- **`SET` is the odd one out.** A value is allowed to contain spaces
  (e.g. `SET greeting hello world`), but `strings.Fields` would have
  already chopped that into `["SET", "greeting", "hello", "world"]`,
  losing the boundary between "the value" and "extra words." So `SET`
  instead re-splits the *original line* with
  `strings.SplitN(line, " ", 3)`, which caps the split at 3 pieces —
  command, key, and *everything else* as one final piece — preserving
  the value's internal spaces.
- **Unknown verbs and malformed known verbs** (e.g. `GET` with no key,
  or the wrong number of arguments) both fall through to the same
  `"ERROR unknown command"` response. The spec only defined that message
  for genuinely unrecognized commands; rather than invent a second,
  unspecified error string for "recognized command, wrong shape," this
  reuses the one the spec already defines.
- Command words are matched via `strings.ToUpper(fields[0])`, so `get`,
  `Get`, and `GET` all work the same — a small robustness touch with no
  real downside.

### `main.go`: just wiring, per the user's request

```go
port := flag.Int("port", 8080, "TCP port to listen on")
capacity := flag.Int("capacity", 1000, "...")
flag.Parse()

store := NewStore(*capacity)
addr := fmt.Sprintf(":%d", *port)
if err := ListenAndServe(addr, store); err != nil {
	log.Fatalf("server error: %v", err)
}
```

`flag.Int` registers a command-line flag and returns a `*int` pointing at
its parsed value (hence `*port`, `*capacity` — dereferencing the pointer
to get the actual number) — you must call `flag.Parse()` before those
pointers hold real values, since that's the call that actually reads
`os.Args`. `-capacity` was added alongside `-port` because `NewStore` (from
Milestone 2) requires a capacity argument; rather than hardcode a hidden
number, it's exposed the same way `-port` is. `log.Fatalf` prints the
error and exits the process (`os.Exit(1)`) if the server can't start at
all (e.g. the port is already in use) — appropriate here since there's
nothing else for `main` to do if listening fails.

### Manually verified against a running server

Since this milestone's whole point is being driven by real client
connections instead of in-process calls, verification used an actual TCP
client (a short PowerShell script using `System.Net.Sockets.TcpClient`,
since this environment has no `nc`/`telnet`) against the running server:

```
SET foo bar baz  =>  OK
GET foo          =>  bar baz        (multi-word value preserved)
GET missing      =>  NOT_FOUND
DELETE foo       =>  OK
GET foo          =>  NOT_FOUND      (confirmed gone after delete)
BOGUS command    =>  ERROR unknown command
```

And with two simultaneous connections, each `SET` from one client was
immediately visible to a `GET` from the *other* client — confirming both
that the goroutine-per-connection model actually serves multiple clients
at once, and that they're all sharing the same underlying `Store`.

### Verified

```
go build ./...              # compiles cleanly
go vet ./...                # no suspicious constructs
go run . -port 8080         # starts and logs "listening on :8080"
(manual TCP client tests above, including concurrent connections)
```

## Milestone 4 — Leader/follower asynchronous replication

**Files:** [replication.go](replication.go), [server.go](server.go), [main.go](main.go)

### One binary, two roles

Nothing here is a second program. The same binary becomes a leader or a
follower based on `-role`, and both roles run the *same* `Store` and the
*same* TCP accept loop from Milestones 1-3. What changes is only which
commands the server is willing to accept, and whether it forwards writes
onward. `store.go` is still untouched — three milestones running now.

`server.go` did change shape, though. Milestones 1-3 got by with plain
functions taking a `*Store`, but a node now carries three things that
travel together: its store, its role, and (on a leader) its replicator.
Threading three parameters through `ListenAndServe` → `handleConnection`
→ `handleCommand` would be noise, so those became methods on a small
`Server` struct. That's the usual Go answer to "these functions all need
the same handful of state."

### Why a follower refuses even `GET`

The spec for this milestone is that a follower accepts *only* `REPLICATE`.
That's stricter than it first looks: a follower rejects `GET` too, so
there is exactly one way for data to enter a follower's store —
replication from the leader. Any divergence between leader and follower
is therefore a bug in replication, not something a stray client could
have caused by writing to a replica directly. Followers serving reads is
a later concern, and it can't be answered honestly until the staleness
question below is on the table.

Symmetrically, the leader rejects `REPLICATE`: it's an internal
leader-to-follower command, not part of the client protocol.

### `REPLICATE` is just a wrapper

The wire format is `REPLICATE SET key value` / `REPLICATE DELETE key` —
literally the client's own write command with a prefix. That means the
follower can strip `REPLICATE ` and hand the remainder to the exact same
parser the leader used:

```go
func (s *Server) applyWrite(line string) (response string, applied bool)
```

Both the leader (for client writes) and the follower (for replicated
writes) call `applyWrite`. Sharing it is a correctness property, not just
tidiness: if the leader and follower parsed the wire format even slightly
differently — say, one preserving spaces in a multi-word value and the
other not — the replica would silently drift from the leader. One parser
means that can't happen.

`applyWrite` also returns whether the write actually happened, which is
what gates replication: a malformed command the leader rejected must
never be forwarded to a replica.

### The asynchronous part, and what it costs

This is the heart of the milestone. `Replicate()` puts the write on a
queue and returns immediately; the leader has already answered `OK` to
the client before any follower has seen the write.

The upside is that a client's write latency is the leader's local map
write and nothing else — a slow, distant, or completely dead follower
cannot slow down or fail a client request. That's measurable: with one
live follower and one dead one, client writes still returned `OK` in
under a millisecond while the dead follower's dial errors piled up in the
log.

The costs are real and worth naming precisely:

- **Stale reads.** Write to the leader, immediately read that key from a
  follower, and you may see the old value or nothing at all — the
  `REPLICATE` may still be queued or in flight. Usually sub-millisecond,
  but unbounded if a follower is lagging. There is no read-your-own-writes
  guarantee across nodes.
- **Acknowledged writes can be lost.** If the leader crashes after saying
  `OK` but before the write reaches followers, it's gone. The client was
  told it succeeded. The fix is synchronous replication — wait for a
  quorum before answering — which puts follower latency on every write.
  That's the trade this milestone deliberately doesn't make.
- **Permanent divergence.** A follower down long enough to overflow its
  queue has writes dropped, and nothing here ever catches it back up.
  Confirmed in testing: a write made while a follower was down never
  reached it, even after it restarted and later writes flowed normally.
  Real systems need anti-entropy or log shipping for this.

### Why a queue per follower, not a goroutine per write

The obvious way to "not wait" is `go send(write)` at the call site. That
would be wrong, and the reason is ordering. Two goroutines racing to the
same follower can arrive in either order, so a `SET k v` followed by a
`DELETE k` could land reversed — leaving the follower holding a key the
leader deleted, permanently. Since writes are never re-sent, a single
reordering is forever.

So each follower gets one buffered channel and exactly one sender
goroutine draining it. One goroutine writing to one connection means
writes hit the wire in the order the leader applied them. It also means
`conn` and `reader` are owned by that goroutine alone and need no mutex.

The channel is bounded (1024). Unbounded would let a dead follower's
backlog grow until the leader ran out of memory. When it's full,
`enqueue` drops the write and logs it rather than blocking — blocking
would push follower latency back onto the client, which is the one thing
this design exists to avoid.

### Connection reuse and recovery

`send` dials lazily and keeps the connection for subsequent writes, so
the steady state is one long-lived TCP connection per follower rather
than a fresh handshake per write. On any error the connection is closed
and nulled, so the next write redials — no retry logic, no backoff, just
"try again on the next write." Verified: killing a follower produced an
error on the next write, and restarting it caused the very next write to
reconnect silently.

The sender *does* read the follower's `OK` back, which looks like waiting
but costs the client nothing — it happens on the follower's own goroutine,
long after the leader replied. It's what lets the leader notice a
follower that's refusing writes instead of assuming success.

### Verified

Two followers (9001, 9002) and a leader (9000) as separate processes:

```
leader:   SET foo bar baz => OK, GET foo => bar baz, DELETE foo => OK
leader:   REPLICATE SET x 1        => ERROR unknown command  (internal only)
follower: GET/SET/DELETE/BOGUS     => ERROR unknown command  (replica is not writable)
follower: REPLICATE SET k hello world => OK
follower: REPLICATE DELETE k          => OK
```

Replication to both followers produced no errors in the leader's log; a
follower answers `OK` only after `applyWrite` has actually touched its
`Store`, so the acknowledgement is the evidence the write landed.
(A follower can't be read directly to check — that's the point of it
refusing `GET`.)

Failure and recovery behavior:

```
dead follower:     client writes still OK in <1ms, dial errors logged per write
follower killed:   next write logs a write error, client unaffected
follower restarted: next write reconnects silently, no further errors
                   (the write made during the outage is never caught up)
```

```
gofmt -l .    # clean
go build ./...
go vet ./...
```

## Milestone 5 — Heartbeats and Raft-style leader election

**Files:** [node.go](node.go), [server.go](server.go), [main.go](main.go)

### The role stopped being a flag

Milestone 4 decided roles at startup: you launched a node with
`-role leader` and it led forever. That is not a cluster, it is a
hierarchy someone drew by hand. Now every node starts as a follower at
term 0 and the cluster decides for itself who leads, so `role` moved out
of the startup flags and into `Node` as mutable state, alongside `term`
and `votedFor`.

That forced two flags to change with it. `-role` is gone entirely, and
`-followers` (which only made sense if you already knew who the leader
was) became `-cluster`: the address of every node, handed to every node
unchanged. Each node subtracts its own `-addr` to get its peers. `-port`
became `-addr`, because a node in a cluster needs an identity other
nodes can name, not just a port it happens to listen on.

### Terms are the whole trick

A term is a number that only ever goes up, and it is what makes a
distributed algorithm out of what would otherwise be a shouting match.
Every message carries the sender's term, and the rule is unconditional:
**see a higher term anywhere, immediately become a follower at that
term.** Not "consider it" — a higher term is proof this node's view of
the world is stale, whatever it currently believes about itself.

That one rule collapses most of the hard cases. A leader that was
network-partitioned and comes back finds a higher term and steps down
without any special reconnect logic. A candidate that loses learns it
lost from the first message it gets. This is why `observeTerm` is called
on *responses* too, not just requests: a peer's reply carries its term,
so even a rejected heartbeat teaches the sender that it has been
deposed.

### Why the timeout has to be random

Each node picks a fresh random election timeout between 150-300ms every
time the countdown restarts. The randomness is not a rough edge, it is
load-bearing.

If every node used the same timeout, a dead leader would cause all of
them to time out at the same instant, all become candidates in the same
term, and all vote for themselves. Nobody reaches a majority, so all of
them time out again together, and again — a **split vote** that can
repeat indefinitely. Staggering the timeouts means one node almost always
wakes first and has collected a majority before the others have even
started, which is exactly what the logs show: the winner goes from
candidate to leader in about one millisecond.

The two constants are chosen relative to each other. Heartbeats go out
every 50ms against a 150ms floor, so a healthy leader gets roughly three
chances to reset each follower's countdown before anyone gives up on it.
If the heartbeat interval crept above the timeout floor, followers would
constantly depose a perfectly healthy leader.

### One vote per term is what prevents two leaders

`votedFor` is cleared whenever the term advances and set at most once
within a term. A node grants its vote to the first candidate that asks in
a given term and refuses everyone else in that same term.

That is the entire safety argument for leadership. Winning requires a
strict majority, and any two majorities of the same set must share at
least one member. That shared member only voted once, so it cannot have
helped both candidates win. Two nodes therefore cannot both be leader in
the same term — no split brain, without any node needing to know what
the others are doing.

Demonstrated directly by killing two of three nodes: the survivor
becomes a candidate every ~200ms, fails with `1/3 votes (needed 2)`, and
climbs through terms 12, 13, 14, 15 without ever leading. Critically it
also refuses client writes the whole time, because it is a candidate and
only a leader serves writes. A lone node cannot appoint itself and start
accepting data the rest of the cluster will never see.

### Why a stale heartbeat must not reset the timer

The spec says to reset the election timeout whenever a heartbeat or vote
request arrives. The implementation adds one condition: the message must
carry a term at least as high as this node's.

Without that condition there is a real liveness bug. Picture a deposed
leader still at term 3 while the cluster has moved to term 5. Its
heartbeats keep arriving, and if they reset timers, they would suppress
elections. As long as the term-5 leader is alive nothing looks wrong —
but the moment it dies, the stale term-3 leader's heartbeats would keep
every remaining node from ever timing out, and the cluster would sit
leaderless forever, held hostage by a node with no authority. So a
lower-term heartbeat gets an `ACK` carrying the real term (which makes
the sender step down) and pointedly does *not* reset the countdown.

### Where the state machine lives

`Node` holds `term`, `role`, and `votedFor` under a single mutex, because
they always change together — a transition that updated the term but not
`votedFor` would let a node vote twice in one term. Two goroutines drive
everything:

- `runElectionTimer` waits on a randomized timeout or a reset signal,
  whichever comes first, and starts an election if the timeout wins.
  Resets arrive over a buffered channel so a request handler never blocks
  on the timer.
- `runHeartbeats` ticks every 50ms forever and sends heartbeats only on
  the ticks where this node happens to be leader. One permanent ticker is
  easier to reason about than starting and stopping a goroutine on every
  role change, and a no-op tick costs nothing.

Election RPCs go out one goroutine per peer, and a `TryLock` on each
peer's connection skips a send when the previous one is still in flight
— without it, heartbeats every 50ms against a peer that takes up to
100ms to fail would pile up goroutines faster than they drain.

After collecting votes, the result is thrown away unless the node is
*still* a candidate in the *same* term it started. Otherwise a slow
election could crown a node for a term it had already left.

### Known simplifications

- **No log, so no log-completeness check when voting.** Real Raft
  refuses to vote for a candidate whose log is behind the voter's, which
  is what guarantees a new leader holds every committed write. Here any
  node can win any election, so a node that missed writes can be elected
  and those writes are simply gone. Combined with the asynchronous
  replication from Milestone 4, an elected leader is not guaranteed to
  have the previous leader's most recent writes.
- **Votes are counted after every peer replies**, rather than the moment
  a majority arrives. Bounded by the 100ms RPC timeout, so it stays well
  inside one election round, but it is slower than it needs to be.
- **Nothing is persisted.** A node restarts at term 0 and learns the real
  term from the first heartbeat. Real Raft writes `term` and `votedFor`
  to disk before responding, so a crashed node cannot forget a vote and
  cast a second one in the same term.

### Verified

Three nodes on 9101-9103, started a fraction of a second apart:

```
13:16:57.265  [9101 term 1 candidate] election timeout fired, became candidate
13:16:57.266  [9102 term 1 follower]  voted for 9101
13:16:57.266  [9101 term 1 leader]    won election with 2/3 votes
13:16:57.346  [9103 term 1 follower]  term advanced: heartbeat from 9101
```

The logs then go silent, which is the point: heartbeats keep resetting
the timers, so no further elections happen while the leader is healthy.

Killing the leader:

```
13:17:21.498  [9103 term 2 candidate] election timeout fired, became candidate
13:17:21.498  [9102 term 2 follower]  voted for 9103
13:17:21.499  [9103 term 2 leader]    won election with 2/3 votes
```

The failover itself took 1.2ms once the timeout fired. Restarting the
dead node brought it back at term 0, and a heartbeat moved it to term 2
as a follower within 18ms without disturbing the leader.

Client behavior follows the election: `SET`/`GET`/`DELETE` work on
whichever node currently leads, and the others answer
`ERROR unknown command`. Replication continues to log errors for peers
that are down without failing the client.

```
gofmt -l .        # clean
go build ./...
go vet ./...
go build -race    # 3-node cluster, leader killed mid-load,
                  # 3600 concurrent writes: no data races reported
```

## Milestone 6 — TTL expiry and the background reaper

**Files:** [store.go](store.go), [server.go](server.go)

### Two different jobs: staying correct, and reclaiming space

TTL looks like one feature but it splits cleanly into two, and keeping
them separate is what makes the design simple.

**Correctness** is `Get`'s job. When a key's deadline has passed, `Get`
removes it and answers `NOT_FOUND` on the spot, without consulting the
reaper. This has to happen here, because the reaper only wakes once a
second: between a key's deadline and the next sweep, the entry is still
sitting in the map, and handing it back would return a value the client
was promised had already gone. Checking on read makes the deadline exact
from a reader's point of view no matter how lazily the reaper runs.

**Reclaiming space** is the reaper's job. Lazy expiry alone would leave a
key that nobody ever reads again in the map forever, holding memory and —
worse in this store — holding one of the LRU capacity slots. The reaper
exists for exactly the keys that lazy expiry can never reach.

That division means the reaper's interval is a tuning knob, not a
correctness parameter. Making it 10 seconds would waste more memory; it
would not make a single expired key readable.

### An absolute deadline, not a duration

`entry` stores `expiresAt time.Time`, computed once at `Set` time, rather
than the `ttl` the caller passed. A stored duration would need a "stored
at" timestamp next to it and a subtraction on every read, and the answer
would depend on when someone happened to look. A deadline is just a
comparison. The zero `time.Time` means "never expires", which is what
`ttl == 0` stores — so the no-expiration path costs nothing and needs no
extra flag.

Overwriting a key replaces its deadline too, so a plain `SET` clears an
expiration that an earlier `SET ... EX` had put there. That matches
Redis, where a `SET` without `EX` discards any existing TTL.

### The race the mutex prevents

The reaper is the first thing in this project that mutates the store
without a client asking it to. It runs on its own goroutine, on a timer,
while client goroutines are in `Get`, `Set`, and `Delete` — so it takes
`s.mu` exactly like they do.

The race it prevents is not subtle or theoretical. The reaper walks
`s.items` deleting keys while a client goroutine reads or writes that
same map. Go maps are not safe for concurrent use with any writer
present: this is not a stale-value problem, it is a runtime crash
(`concurrent map read and map write`) or silent corruption of the map's
internal buckets.

The recency list is just as exposed, and in a way that is easier to
picture. Every key lives in two structures at once, so removing one means
mutating both. If the reaper called `ll.Remove(elem)` at the same instant
a client's `Get` called `ll.MoveToFront(elem)` on that same element, the
two would be rewriting the same `next`/`prev` pointers with no ordering
between them — leaving a node unreachable, linked to itself, or pointing
at a node that was already removed. Nothing would crash immediately; the
list would just be quietly wrong, and a later eviction would follow a
corrupt pointer.

Holding the one mutex for the whole sweep makes each pass atomic with
respect to every client operation: a client sees the store either
entirely before or entirely after a sweep, never midway through it.

Deleting from a map while ranging over it — which the sweep does — is
explicitly allowed in Go: entries removed during the loop are simply not
produced later.

### `SET key value EX seconds`, and an ambiguity worth admitting

The wire format follows Redis. `EX` is optional, and its absence means no
expiration.

Parsing it collides with a decision from Milestone 3: values may contain
spaces, which is why `SET` splits the raw line into exactly three pieces
instead of tokenizing it. Now the third piece may or may not end with two
option words. The parser looks at the last two words, and treats them as
an option only when the second-to-last is `EX` and the last parses as a
number.

That leaves a genuine ambiguity: `SET note remind me EX 60` stores
`remind me` with a 60-second expiration, not the literal text
`remind me EX 60`. There is no way around it in a protocol that splits on
spaces — Redis avoids it by framing arguments in its wire format rather
than by splitting a line. The trailing-option reading is the useful one,
and the alternative would mean inventing quoting rules this protocol does
not have. It is documented on the parser rather than hidden.

`EX` followed by a non-number is rejected rather than treated as part of
the value, since silently storing a line the client meant as an expiring
write is worse than an error. `EX 0` and negatives are rejected too:
zero already means "no expiration" internally, so accepting `EX 0` would
do the precise opposite of what a client asking for an expiry wants.

### How this rides along with replication

Nothing in [replication.go](replication.go) changed. The leader forwards
the client's raw line, so `SET k v EX 30` reaches followers with its `EX`
intact and is parsed there by the same `applyWrite` the leader used.

One caveat that falls out of that: the follower computes its deadline
when it *applies* the write, so its expiry is a fraction of a second
later than the leader's. Under asynchronous replication that gap is
normally sub-millisecond, but it is unbounded if a follower is lagging.
A system that cared would replicate the absolute deadline instead of the
duration — which then needs the nodes' clocks to agree, a much larger
problem than it looks.

### Verified

Protocol parsing, against a single node:

```
SET plain hello world        => OK        GET plain => hello world
SET exp hello world EX 5     => OK        GET exp   => hello world   (spaces kept)
SET bad v EX abc             => ERROR unknown command
SET zero v EX 0              => ERROR unknown command
SET neg v EX -3              => ERROR unknown command
```

**Expiry is lazy, not reaper-driven.** A key was set with `EX 1` and then
polled every 5ms until it vanished, repeated with the `SET` deliberately
landing at different phases of the reaper's one-second tick:

```
round 0: flipped at 1.010s     round 3: flipped at 1.008s
round 1: flipped at 1.014s     round 4: flipped at 1.015s
round 2: flipped at 1.015s
```

Every round flips within ~15ms of the deadline. If the reaper were doing
the work, these would scatter anywhere up to a full second late
depending on tick phase.

**The reaper really does reclaim capacity.** With `-capacity 2`: set
`live`, then set `x` with `EX 1` and never read it, so lazy expiry can
never fire on it. After waiting 2.5s, a third key was added and
`GET live` still returned its value — meaning `x` had been swept and the
store was below capacity. Had `x` still occupied a slot, the new key
would have evicted `live`, which was the least recently used.

**Reaper against live traffic, under the race detector.** Eight
concurrent clients ran `SET ... EX 1` / `GET` / `DELETE` for six seconds
against a race-enabled node, so the reaper swept roughly six times
mid-traffic:

```
WARNING: DATA RACE occurrences: 0
```

**Replication carries the TTL.** In a two-node cluster the leader
accepted `SET rep hello world EX 30` and logged no replication errors —
and a follower only answers `OK` after `applyWrite` has actually applied
the write, so the `EX` form parsed and applied on the replica too.

```
gofmt -l .    # clean
go build ./...
go vet ./...
```

## Milestone 7 — Benchmark and failure-mode scripts

**Files:** [cmd/bench/main.go](cmd/bench/main.go), [cmd/failover/main.go](cmd/failover/main.go)

Both are Go programs under `cmd/`, so the whole project stays one
language and one toolchain. They are separate `main` packages, which is
why they can live in the same module as the server without colliding
with its `main`.

### Finding the leader without being told

Both scripts need "whichever node is leading right now", and elections
move that around. Rather than take it as a flag, they probe: send
`GET __bench_leader_probe__` to each address and watch who answers.

A leader replies `NOT_FOUND`; a follower or candidate replies
`ERROR unknown command`, because non-leaders refuse client commands.
That distinction, which exists for correctness reasons from Milestone 4,
turns out to double as leader discovery. The probe key is never written,
so asking costs nothing and changes nothing.

### bench: what the numbers mean

`-clients` connections each loop sending a mix of `SET` and `GET`
(`-read-ratio`) over `-duration`, and every operation's latency is
recorded. Percentiles use the nearest-rank method on the sorted samples.

Each client collects into its own slice with no shared counter, so the
measurement doesn't serialize the thing it is measuring.

`-pipeline` is the sync/async knob on the *client* side. At 1, a client
sends one command and waits for its reply — strict request/response. Above
1, that many requests may be outstanding at once. The protocol answers
one line per command in order, so responses match requests by position
and no request IDs are needed: a sender goroutine pushes send-timestamps
into a buffered channel whose capacity *is* the pipeline depth, and a
reader goroutine pops them as replies arrive. The channel doing double
duty as the in-flight limit is what keeps this short.

The result at 50 clients was a textbook queueing outcome:

```
pipeline 1    97,523 ops/sec    p50 1.098ms   p99  6.052ms
pipeline 16  101,551 ops/sec    p50 8.496ms   p99 18.718ms
```

Pipelining bought about 4% more throughput for roughly 8x the latency.
The server was already saturated at 50 concurrent clients, so deeper
queues added waiting, not work. Worth knowing before reaching for
pipelining as an optimization.

The sync-vs-async comparison the numbers actually support is on the
server side, and it shows in the split rows:

```
SET   p50 1.665ms   p99 6.585ms
GET   p50 0.557ms   p99 4.911ms
```

`SET` costs more than `GET` — it mutates and enqueues replication work —
but it is the same order of magnitude, because replication is
asynchronous and no follower round trip is on this path. Synchronous
replication would add at least one leader-to-follower round trip to
every `SET`, and would tie `SET` latency to the *slowest* follower rather
than to the leader alone.

### failover: two bugs the first run exposed

The script starts a 3-node cluster, streams `SET`s at the leader,
kills the leader partway through, waits for a new one, and reads back
every key the old leader had acknowledged. A key is recorded only after
`OK` comes back, so the verified set is exactly what a client was
promised.

The first version reported 44,690 of 45,690 writes lost — and the
number that gave it away was the 1,000 that survived, which is exactly
the nodes' default `-capacity`. The test had written 45,690 distinct keys
into a 1,000-key store, so LRU eviction had discarded nearly all of them
before any failover happened. The script now starts nodes with a large
`-capacity` and refuses to run if the write count exceeds it, because a
missing key would otherwise say nothing about replication.

The second bug was quieter. `os.Exit` skips deferred functions, so every
`FAIL` path exited without running `defer stopAll(nodes)` — leaking three
node processes per run and leaving ports 9601-9603 occupied for the next
one. Since `FAIL` is a *likely* outcome of this test, that was every
other run. `main` now does nothing but `os.Exit(run())`, and `run`
returns exit codes so the defers fire.

A third thing needed a workaround rather than a fix: on Windows,
`Process.Kill()` came back with `TerminateProcess: Access is denied` for
a process the script had started itself. That silently turned the test
into a no-op — the leader kept running, no election happened, and the
run failed for reasons unrelated to the cluster. `terminate` now falls
back to `taskkill /F /T /PID`, which the OS does allow.

### What the failover test actually shows

Across four runs it passed three times and failed once:

```
run 1   45,983 acked   17 missing    FAIL (0.04%)
run 2   46,160 acked    0 missing    PASS
run 3   31,739 acked    0 missing    PASS
run 4   31,664 acked    0 missing    PASS
```

The failing run is the interesting one, and not because it failed: the
17 missing keys were `failover:45966` through the end — the *last* writes
before the kill, contiguous, with nothing missing earlier. That is
precisely the shape asynchronous replication predicts. The leader
acknowledged those writes when they hit its own Store; they were still
queued or in flight when it died; nothing re-sends them; and because
there is no replicated log, the election could not prefer a node that
had them.

The window is narrow — replication drains in well under a millisecond on
loopback, so only about a millisecond of writes is ever at risk, which is
why most runs lose nothing. That is worth stating plainly: **a PASS here
does not demonstrate durability.** It means nothing happened to be in
flight at the instant the process died. The guarantee is absent either
way; the test only sometimes catches it.

### Running them

```
go run ./cmd/failover
go run ./cmd/failover -duration 8s -kill-after 4s

# bench needs a cluster already running
go run ./cmd/bench -cluster 127.0.0.1:9701,127.0.0.1:9702,127.0.0.1:9703 \
    -clients 50 -duration 10s -pipeline 1
```

`failover` is self-contained: it builds the server, starts the cluster on
`-base-port` and up, and cleans up after itself.

```
gofmt -l .    # clean
go build ./...
go vet ./...
```

## Cleanup pass

**Files:** [main.go](main.go), [README.md](README.md), [.gitignore](.gitignore)

With all seven stages done, a pass over the whole project looking for code
that had stopped earning its place.

Every top-level declaration across the seven files was listed and its
references counted. Nothing was dead: every function, type and constant is
reachable, and no struct field is unused. Go's compiler catches unused
imports and locals but says nothing about an unused package-level function,
so this needed checking rather than assuming.

One thing did go. `clusterPeers` reported a bad `-cluster` list through a
dedicated error struct with its own `Error` method -- twelve lines and a
type to carry two strings to a single call site that only ever printed
them. `fmt.Errorf` says the same thing in one statement.

Three pieces of duplication were left alone on purpose, since removing them
would be a refactor with real risk rather than a cleanup:

- **`follower` in [replication.go](replication.go) and `peer` in
  [node.go](node.go) are near-duplicates.** Both manage a persistent
  line-protocol connection with lazy redial. But they differ where it
  matters: `follower` has a queue and a sender goroutine and no socket
  deadlines, while `peer` has a `TryLock` and a 100ms deadline. Merging them
  means reconciling two different failure models on the replication and
  election hot paths, which wants tests behind it first.
- **`probe` is copy-pasted between the two commands.** Extracting it would
  stop each script being a single readable file, which was the point of
  writing them that way.
- **`applyWrite` re-parses a line `handleCommand` already tokenized.**
  Redundant work on the write path, but it is exactly what guarantees a
  leader and a replica interpret the wire format identically. Removing the
  redundancy would trade a safety property for a small win.

`README.md` was added at the same time, covering what the project does now:
the protocol, the flags, how to run a cluster, both tools with their real
output, and the known limitations. And `NOTES.md` -- this file -- stopped
being gitignored. It had been untracked since early on, which meant the most
useful thing in the repository was invisible to anyone who opened it.
