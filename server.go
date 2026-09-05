package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

// ListenAndServe listens for TCP connections on addr and serves the
// SET/GET/DELETE protocol against store until the listener fails.
func ListenAndServe(addr string, store *Store) error {
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

		go handleConnection(conn, store)
	}
}

// handleConnection serves one client connection: read a line, run it as a
// command against store, write back a response, repeat until the client
// disconnects.
func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		response := handleCommand(scanner.Text(), store)
		fmt.Fprintf(conn, "%s\n", response)
	}
}

// handleCommand parses a single protocol line and runs it against store,
// returning the response line to send back.
func handleCommand(line string, store *Store) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "ERROR unknown command"
	}

	switch strings.ToUpper(fields[0]) {
	case "GET":
		if len(fields) != 2 {
			return "ERROR unknown command"
		}
		value, ok := store.Get(fields[1])
		if !ok {
			return "NOT_FOUND"
		}
		return value

	case "SET":
		// The value may itself contain spaces, so split the raw line into
		// at most 3 pieces (command, key, value) instead of using fields,
		// which would have already broken a multi-word value apart.
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return "ERROR unknown command"
		}
		store.Set(parts[1], parts[2])
		return "OK"

	case "DELETE":
		if len(fields) != 2 {
			return "ERROR unknown command"
		}
		store.Delete(fields[1])
		return "OK"

	default:
		return "ERROR unknown command"
	}
}
