# kvstore

A distributed in-memory key-value store in Go, built from scratch with no
dependencies outside the standard library. Nodes form a small cluster that
elects its own leader using a simplified Raft-style protocol, replicates
writes from the leader to its followers, and speaks a plain-text line
protocol over TCP.

This is a learning project. It is written to be read: the interesting
decisions are explained in comments at the place they were made, and the
limitations are documented rather than hidden.

## What it does

- **In-memory key-value store** with `SET` / `GET` / `DELETE`, safe for
  concurrent use.
- **LRU eviction** at a configurable capacity, using a map plus a
  `container/list` recency list so both lookup and eviction are O(1).
- **TTL expiry** with Redis-style `EX seconds`. Reads enforce the deadline
  immediately; a background reaper reclaims space from keys nobody reads
  again.
- **TCP server** speaking one line in, one line out, with a goroutine per
  connection.
- **Leader/follower replication**, asynchronous: the leader answers the
  client as soon as its own store is updated.
- **Leader election** with terms, randomized election timeouts, one vote
  per term, and majority quorum — so a dead leader is replaced
  automatically and two nodes cannot lead the same term.

## Requirements

Go 1.27.1 or newer (declared in `go.mod`). No external dependencies.

## Quick start

Build the binary:

```bash
go build -o kvstore .
```

Run a single node:

```bash
./kvstore -addr 127.0.0.1:9000
```

A lone node elects itself and starts serving immediately. Connect with any
line-oriented TCP client and type commands:

```
SET greeting hello world
OK
GET greeting
hello world
SET session abc123 EX 30
OK
GET missing
NOT_FOUND
```

### Running a three-node cluster

Every node gets the same `-cluster` list — including its own address — and
identifies itself with `-addr`. Run each in its own terminal:

```bash
./kvstore -addr 127.0.0.1:9001 -cluster 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
```

```bash
./kvstore -addr 127.0.0.1:9002 -cluster 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
```

```bash
./kvstore -addr 127.0.0.1:9003 -cluster 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
```

Within a few hundred milliseconds one of them wins an election and logs it:

```
[127.0.0.1:9001 term 1 candidate] election timeout fired, became candidate and voted for itself
[127.0.0.1:9001 term 1 leader]    won election with 2/3 votes, became leader
```

Kill the leader and watch the survivors elect a new one. Every state
transition is logged with a microsecond timestamp, because an election
completes in about a millisecond.

### Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | This node's address. It listens here and identifies itself by this in the cluster. |
| `-cluster` | *(empty)* | Comma-separated addresses of every node, including this one. Empty means a single-node cluster. |
| `-capacity` | `1000` | Maximum keys held before LRU eviction. |

A node whose `-addr` is missing from its own `-cluster` list is refused at
startup — it would otherwise break everyone's majority arithmetic.

## Protocol

Text over TCP. One command per line, one response per line. Commands are
case-insensitive; keys and values are not.

| Command | Response |
| --- | --- |
| `SET key value` | `OK` |
| `SET key value EX seconds` | `OK`, and the key expires after that many seconds |
| `GET key` | the value, or `NOT_FOUND` |
| `DELETE key` | `OK` (whether or not the key existed) |
| anything else | `ERROR unknown command` |

Values may contain spaces: `SET greeting hello world` stores `hello world`.

**Only the leader serves client commands.** A follower or candidate answers
`ERROR unknown command` to everything above, so replication from the leader
is the only way data enters a replica. There is no redirect yet — a client
has to find the leader itself, which is what the included tools do by
probing.

Three commands are internal to the cluster and are accepted regardless of
role:

| Command | Response | Sent by |
| --- | --- | --- |
| `REPLICATE SET key value` | `OK` | leader, to replicate a write |
| `HEARTBEAT term addr` | `ACK term` | leader, every 50ms |
| `REQUESTVOTE term addr` | `VOTE yes\|no term` | candidate, during an election |

## How it fits together

| File | Responsibility |
| --- | --- |
| `store.go` | The store itself: map + LRU list + TTL, all under one mutex. Includes the reaper goroutine. |
| `server.go` | TCP accept loop, connection handling, command parsing, and role-based gating. |
| `replication.go` | Leader-to-follower replication: one queue and one sender goroutine per peer. |
| `node.go` | Election state machine: term, role, vote, election timer, heartbeat ticker. |
| `main.go` | Flag parsing and wiring. |
| `cmd/bench` | Throughput and latency benchmark. |
| `cmd/failover` | Durability test across a leader kill. |

A few decisions worth calling out:

**One mutex, not a read-write lock.** `Get` mutates shared state — it moves
the key to the front of the recency list and drops it if expired — so there
is no read-only path for an `RLock` to protect.

**Randomized election timeouts are load-bearing.** If every node used the
same timeout, a dead leader would make them all become candidates in the
same instant and split the vote, possibly forever. Staggering them means one
node almost always wins outright.

**One vote per term is the safety argument.** Winning needs a strict
majority, and any two majorities of the same set share a member. That member
voted once, so it cannot have elected two leaders in one term.

**Leader and replica share one parser.** A replicated write goes through the
same code the leader used, so the two cannot drift by interpreting the wire
format differently.

## Included tools

### Benchmark

Point it at a running cluster; it finds the leader itself.

```bash
go run ./cmd/bench -cluster 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
    -clients 50 -duration 10s -pipeline 1
```

Flags: `-clients`, `-duration`, `-pipeline`, `-read-ratio`, `-keys`,
`-value-size`, `-cluster`.

Sample output (50 clients, 10s, 50% reads, three nodes on loopback on one
Windows machine — these are not portable performance claims):

```
results
  total ops     975485
  throughput    97523 ops/sec

latency (ms)        p50       p99       max
  all                1.098     6.052    13.100
  SET                1.665     6.585    13.100
  GET                0.557     4.911    11.474
```

`SET` costs more than `GET` but stays in the same range, because
replication is asynchronous and no follower round trip is on the write
path. Synchronous replication would push `SET` above `GET` by the cost of
the slowest follower.

`-pipeline` controls how many requests a client keeps in flight. At 16 on
the same setup, throughput rose about 4% while p50 latency went from 1.1ms
to 8.5ms — at 50 clients the server is already saturated, so deeper queues
add waiting rather than work.

### Failover test

Self-contained: builds the server, starts a three-node cluster, streams
writes at the leader, kills it, waits for a new leader, and reads back every
acknowledged write.

```bash
go run ./cmd/failover -duration 6s -kill-after 3s
```

```
  writes acknowledged before the kill  45983
  new leader                           127.0.0.1:9603
    [127.0.0.1:9603 term 2 leader] won election with 2/3 votes, became leader

verifying every acknowledged write against the new leader...
  checked        45983
  present        45966
  missing        17

FAIL: 17 of 45983 acknowledged writes were lost (0.04%)
```

**This test is expected to fail sometimes, and that is the point.** Across
four runs it passed three times and failed once. On the failing run the
missing keys were the *last* 17 written, contiguous, with nothing missing
earlier — exactly the shape asynchronous replication predicts. The leader
acknowledged them when they reached its own store; they were still queued
when it died; nothing re-sends them.

A pass does not demonstrate durability. It means nothing happened to be in
flight at the instant the process was killed.

## Known limitations

These are deliberate, and most of them have a named fix.

- **Acknowledged writes can be lost.** Replication is asynchronous, so a
  leader that crashes after answering `OK` takes any un-replicated writes
  with it. Synchronous replication — waiting for a quorum before answering —
  is the trade that fixes it, at the cost of follower latency on every
  write.
- **No replicated log**, so voting has no log-completeness check. Any node
  can win any election, including one that missed writes. Real Raft refuses
  to vote for a candidate whose log is behind the voter's, which is what
  guarantees a new leader holds every committed entry.
- **Nothing is persisted.** A restarted node comes back at term 0 having
  forgotten its vote, so it could vote twice in one term. Real Raft writes
  `term` and `votedFor` to disk before responding.
- **Followers do not serve reads.** They reject `GET` so replication stays
  the only path into a replica.
- **No client redirect.** A client that reaches a non-leader gets
  `ERROR unknown command` with no hint about who the leader is.
- **A lagging follower is never caught up.** If its replication queue
  overflows, writes are dropped and logged, and nothing back-fills them.
- **No persistence, authentication, or TLS**, by design.

## Testing it by hand

Any client that sends a line and reads a line will do — `nc` on Linux or
macOS, for instance. On Windows, `curl.exe --no-buffer telnet://...`
connects but buffers stdin, so nothing is sent until the connection closes;
`python -m telnetlib 127.0.0.1 9000` works interactively instead, on Python
versions before 3.13.

## Design notes

A longer companion document, `NOTES.md`, is kept alongside the code. It
walks through each stage of the build in order and explains the reasoning
behind the design — why the mutex replaced the read-write lock, why the
election timeout has to be random, why a stale heartbeat must not reset the
timer, and what each simplification costs. It is not currently tracked in
git.
