package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

const (
	// replicationQueueSize bounds how many writes may be waiting to go out
	// to a single follower. The bound matters: without it, a follower that
	// is down would let the backlog grow until the leader ran out of
	// memory.
	replicationQueueSize = 1024

	// dialTimeout bounds a connection attempt to a follower, so a follower
	// whose host is unreachable (packets dropped rather than refused)
	// fails in a couple of seconds instead of hanging that follower's
	// sender goroutine for the OS default.
	dialTimeout = 2 * time.Second
)

// Replicator forwards the leader's writes to its followers.
//
// THIS IS ASYNCHRONOUS REPLICATION. Replicate() puts each write on a
// per-follower queue and returns immediately; it does not wait for any
// follower to receive, apply, or acknowledge the write. The leader has
// already answered "OK" to the client by the time a follower sees the
// write at all.
//
// What that buys us: a client's write latency is the leader's local Store
// write and nothing more. A slow, distant, or completely dead follower
// cannot slow down or fail a client's request — which is exactly what the
// milestone asks for.
//
// What it costs us, and this is the important part:
//
//   - Reads from a follower can be stale. A client that writes to the
//     leader and then immediately reads that same key from a follower may
//     see the old value, or no value at all, because the REPLICATE for
//     that write may still be sitting in a queue or in flight. The window
//     is normally sub-millisecond on a healthy network, but it is real,
//     and it is unbounded if a follower is lagging or reconnecting. There
//     is no read-your-own-writes guarantee across nodes.
//
//   - Acknowledged writes can be lost. If the leader crashes after
//     replying "OK" but before the write reaches the followers, that write
//     is gone: it lived only in the leader's memory and its queues. The
//     client was told it succeeded. Synchronous replication (wait for a
//     quorum of followers before answering the client) is the trade that
//     fixes this, at the cost of putting follower latency on every write.
//
//   - Followers can fall permanently behind. If a follower is down long
//     enough to overflow its queue, writes are dropped and logged; nothing
//     in this milestone ever goes back and catches that follower up. A
//     real system needs an anti-entropy or log-shipping mechanism for
//     this, which is out of scope here.
//
// Ordering is preserved per follower, which is why each one gets a single
// sender goroutine draining a queue rather than a fresh goroutine per
// write. Two writes to the same key must land in the order the leader
// applied them; if a SET and a following DELETE raced, a follower could
// end up holding a key the leader deleted, and stay wrong forever.
type Replicator struct {
	followers []*follower
}

// NewReplicator starts one sender goroutine per follower address. It
// returns nil when there are no addresses, so a leader running on its own
// carries no replication machinery at all.
func NewReplicator(addrs []string) *Replicator {
	if len(addrs) == 0 {
		return nil
	}

	r := &Replicator{}
	for _, addr := range addrs {
		f := &follower{
			addr:  addr,
			queue: make(chan string, replicationQueueSize),
		}
		r.followers = append(r.followers, f)
		go f.run()
	}
	return r
}

// Replicate queues command for every follower and returns immediately.
// command is a write command as the client sent it, e.g. "SET key value".
func (r *Replicator) Replicate(command string) {
	for _, f := range r.followers {
		f.enqueue(command)
	}
}

// follower is the leader's side of one leader-to-follower link: a queue of
// pending writes and the connection they go out on.
type follower struct {
	addr  string
	queue chan string

	// conn and reader are touched only by run()'s goroutine, so they need
	// no mutex. The connection is persistent and reused across writes;
	// it is redialed lazily after an error.
	conn   net.Conn
	reader *bufio.Reader
}

// enqueue hands a write to this follower's sender goroutine without
// blocking the caller, which is a client-facing request goroutine.
func (f *follower) enqueue(command string) {
	select {
	case f.queue <- command:
	default:
		// The queue is full, meaning this follower is down or too slow to
		// keep up. Blocking here would push follower latency onto the
		// client, which is the one thing asynchronous replication exists
		// to avoid, so the write is dropped instead. That leaves this
		// follower permanently behind on that key — see the note about
		// catch-up on the Replicator type.
		log.Printf("replication: queue full for follower %s, dropping %q", f.addr, command)
	}
}

// run is the single sender goroutine for one follower. Draining one queue
// from one goroutine is what keeps writes in order on the wire.
func (f *follower) run() {
	for command := range f.queue {
		if err := f.send(command); err != nil {
			// Replication failures are logged and never surfaced to the
			// client, whose write already succeeded on the leader.
			log.Printf("replication: follower %s: %v", f.addr, err)

			// Drop the connection so the next write redials. Whatever
			// went wrong (peer closed, half-open socket, garbage
			// response), a fresh connection is the cheap way back to a
			// known state.
			f.disconnect()
		}
	}
}

// send delivers one write to the follower and waits for its acknowledgement.
// Waiting here costs the client nothing: this runs on the follower's own
// goroutine, long after the leader replied.
func (f *follower) send(command string) error {
	if err := f.connect(); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(f.conn, "REPLICATE %s\n", command); err != nil {
		return fmt.Errorf("writing %q: %w", command, err)
	}

	// The follower answers one line per command, the same as any client
	// connection. Reading it back is what lets us notice a follower that
	// is refusing writes rather than silently assuming success.
	response, err := f.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading response to %q: %w", command, err)
	}

	response = strings.TrimRight(response, "\r\n")
	if response != "OK" {
		return fmt.Errorf("unexpected response %q to %q", response, command)
	}
	return nil
}

// connect dials the follower if there is no live connection, reusing the
// existing one otherwise.
func (f *follower) connect() error {
	if f.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", f.addr, dialTimeout)
	if err != nil {
		return fmt.Errorf("dialing: %w", err)
	}

	f.conn = conn
	f.reader = bufio.NewReader(conn)
	return nil
}

// disconnect closes the current connection so the next send redials.
func (f *follower) disconnect() {
	if f.conn == nil {
		return
	}

	f.conn.Close()
	f.conn = nil
	f.reader = nil
}
