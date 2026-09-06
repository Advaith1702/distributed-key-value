package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

// Role decides which commands a node's TCP server is willing to accept.
type Role string

const (
	// RoleLeader accepts client commands (GET/SET/DELETE) and forwards
	// every write to its followers.
	RoleLeader Role = "leader"

	// RoleFollower accepts only the internal REPLICATE command sent by a
	// leader. A follower is a replica, not an independently writable node.
	RoleFollower Role = "follower"
)

// errUnknownCommand is the single error response the protocol defines. It
// covers both unrecognized verbs and recognized verbs with the wrong shape.
const errUnknownCommand = "ERROR unknown command"

// Server serves the plain-text protocol for one node. Milestones 1-3 got by
// with plain functions taking a *Store, but a node now has a role and
// (on a leader) a replicator to carry alongside it, so those three things
// are grouped here instead of being threaded through every function.
type Server struct {
	store *Store
	role  Role

	// replicator is nil on a follower, and also on a leader started with
	// no -followers. Both cases mean "apply writes locally and stop
	// there", so the nil check lives in one place, in replicate().
	replicator *Replicator
}

// NewServer creates a Server for a node with the given role. replicator may
// be nil, in which case writes are applied locally and not forwarded.
func NewServer(store *Store, role Role, replicator *Replicator) *Server {
	return &Server{
		store:      store,
		role:       role,
		replicator: replicator,
	}
}

// ListenAndServe listens for TCP connections on addr and serves the
// protocol against this node's Store until the listener fails.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	log.Printf("listening on %s", addr)

	// Accept() hands back one connection at a time. If we read and wrote
	// that connection right here in the loop, no other client could
	// connect until the current one finished — a single slow or idle
	// client would stall every other client behind it. Spawning a
	// goroutine per connection lets this loop go straight back to
	// Accept() for the next client, so every connection gets its own
	// concurrent lifeline. The Store's own mutex (from Milestone 1) is
	// what keeps concurrent access from all these goroutines safe.
	for {
		conn, err := ln.Accept()
		if err != nil {
			// A single failed Accept (e.g. a transient resource limit)
			// shouldn't take the whole server down. A production server
			// would distinguish transient errors from fatal ones and back
			// off; that's out of scope here, so we just log and keep
			// accepting.
			log.Printf("accept error: %v", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

// handleConnection serves one connection: read a line, run it as a command,
// write back a response, repeat until the peer disconnects. On a leader the
// peer is a client; on a follower it is the leader's replication
// connection, which is long-lived and reused across many writes.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		response := s.handleCommand(scanner.Text())
		fmt.Fprintf(conn, "%s\n", response)
	}
}

// handleCommand parses a single protocol line and runs it against this
// node, returning the response line to send back.
func (s *Server) handleCommand(line string) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return errUnknownCommand
	}

	verb := strings.ToUpper(fields[0])

	// A follower's whole job is to mirror the leader. It deliberately
	// refuses client commands — including GET — so there is exactly one
	// way for data to enter a follower's Store: replication from the
	// leader. That keeps the replica a faithful copy and makes any
	// divergence a bug rather than something a stray client could cause.
	// (Serving reads from followers is a later milestone; it needs the
	// staleness question below answered first.)
	if s.role == RoleFollower {
		if verb != "REPLICATE" {
			return errUnknownCommand
		}
		return s.handleReplicate(line)
	}

	switch verb {
	case "GET":
		if len(fields) != 2 {
			return errUnknownCommand
		}
		value, found := s.store.Get(fields[1])
		if !found {
			return "NOT_FOUND"
		}
		return value

	case "SET", "DELETE":
		response, applied := s.applyWrite(line)
		if applied {
			// Local Store first, followers second — a write the leader
			// itself rejected must never reach a replica. replicate()
			// only queues the write; it does not wait for followers, so
			// this returns to the client immediately. See Replicator for
			// what that costs us in consistency.
			s.replicate(line)
		}
		return response

	default:
		// REPLICATE lands here on a leader: it is an internal
		// leader-to-follower command, not something a client may send.
		return errUnknownCommand
	}
}

// applyWrite parses and applies a single write command (SET or DELETE) to
// the local Store, reporting the response and whether the write actually
// happened. Both the leader (for client writes) and the follower (for
// replicated writes) go through here, so a replicated command is
// interpreted exactly the way the leader interpreted it.
func (s *Server) applyWrite(line string) (response string, applied bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return errUnknownCommand, false
	}

	switch strings.ToUpper(fields[0]) {
	case "SET":
		// The value may itself contain spaces, so split the raw line into
		// at most 3 pieces (command, key, value) instead of using fields,
		// which would have already broken a multi-word value apart.
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return errUnknownCommand, false
		}
		s.store.Set(parts[1], parts[2])
		return "OK", true

	case "DELETE":
		if len(fields) != 2 {
			return errUnknownCommand, false
		}
		s.store.Delete(fields[1])
		return "OK", true

	default:
		return errUnknownCommand, false
	}
}

// handleReplicate applies a write the leader forwarded. The line looks like
// "REPLICATE SET key value" — strip the REPLICATE prefix and the remainder
// is an ordinary write command, which applyWrite already knows how to read.
func (s *Server) handleReplicate(line string) string {
	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		return errUnknownCommand
	}

	command := strings.TrimLeft(parts[1], " ")
	response, _ := s.applyWrite(command)
	return response
}

// replicate forwards a write to this leader's followers, if it has any.
func (s *Server) replicate(command string) {
	if s.replicator == nil {
		return
	}
	s.replicator.Replicate(command)
}
