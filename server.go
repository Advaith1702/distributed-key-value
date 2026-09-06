package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

// errUnknownCommand is the single error response the protocol defines. It
// covers both unrecognized verbs and recognized verbs with the wrong shape.
const errUnknownCommand = "ERROR unknown command"

// Server serves the plain-text protocol for one node. It owns no role of
// its own any more: the role lives in Node, changes as elections happen,
// and is read fresh on every command.
type Server struct {
	store *Store
	node  *Node

	// replicator is nil when this node has no peers. Every node has one,
	// because every node may be elected leader, but it is only ever used
	// while this node actually is the leader.
	replicator *Replicator
}

// NewServer creates a Server for one node. replicator may be nil, in which
// case writes are applied locally and not forwarded.
func NewServer(store *Store, node *Node, replicator *Replicator) *Server {
	return &Server{
		store:      store,
		node:       node,
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
	// connect until the current one finished -- a single slow or idle
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
// write back a response, repeat until the peer disconnects. The peer may be
// a client, a leader replicating a write, or a node sending heartbeats and
// vote requests -- they all speak the same line protocol on the same port.
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

	// Election messages are served by every node in every role, and are
	// checked before anything else. That is the point of "any node could
	// become leader": a node cannot refuse to take part in an election
	// just because of the role it happens to hold right now, and a
	// candidate still has to answer heartbeats from the node that beat it.
	switch verb {
	case "HEARTBEAT":
		return s.handleHeartbeat(fields)
	case "REQUESTVOTE":
		return s.handleRequestVote(fields)
	}

	// Everything below depends on whether this node is currently the
	// leader, which an election may have changed a millisecond ago.
	if s.node.Role() != RoleLeader {
		// A non-leader is a replica: it applies what the leader sends and
		// nothing else. Client commands, GET included, are refused so
		// that replication stays the only path by which data enters a
		// replica's Store, and any divergence is a replication bug rather
		// than something a stray client caused.
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
			// Local Store first, followers second -- a write the leader
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

// handleHeartbeat parses "HEARTBEAT <term> <addr>" and hands it to the node.
func (s *Server) handleHeartbeat(fields []string) string {
	if len(fields) != 3 {
		return errUnknownCommand
	}

	term, err := strconv.Atoi(fields[1])
	if err != nil {
		return errUnknownCommand
	}

	return s.node.HandleHeartbeat(term, fields[2])
}

// handleRequestVote parses "REQUESTVOTE <term> <addr>" and hands it to the
// node.
func (s *Server) handleRequestVote(fields []string) string {
	if len(fields) != 3 {
		return errUnknownCommand
	}

	term, err := strconv.Atoi(fields[1])
	if err != nil {
		return errUnknownCommand
	}

	return s.node.HandleRequestVote(term, fields[2])
}

// applyWrite parses and applies a single write command (SET or DELETE) to
// the local Store, reporting the response and whether the write actually
// happened. Both the leader (for client writes) and a replica (for
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
// "REPLICATE SET key value" -- strip the REPLICATE prefix and the remainder
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

// replicate forwards a write to this node's peers, if it has any.
func (s *Server) replicate(command string) {
	if s.replicator == nil {
		return
	}
	s.replicator.Replicate(command)
}
