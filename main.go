package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
)

func main() {
	port := flag.Int("port", 8080, "TCP port to listen on")
	capacity := flag.Int("capacity", 1000, "maximum number of keys the store holds before LRU eviction")
	role := flag.String("role", string(RoleLeader), "node role: leader or follower")
	followerList := flag.String("followers", "", "comma-separated follower addresses (host:port) to replicate writes to; leader only")
	flag.Parse()

	nodeRole := Role(strings.ToLower(*role))
	if nodeRole != RoleLeader && nodeRole != RoleFollower {
		log.Fatalf("invalid -role %q: must be %q or %q", *role, RoleLeader, RoleFollower)
	}

	followers := parseFollowers(*followerList)
	if nodeRole == RoleFollower && len(followers) > 0 {
		// A follower has no followers of its own: replication is
		// leader-to-follower only, so this combination is a mistake worth
		// catching at startup rather than silently ignoring.
		log.Fatalf("-followers is only valid with -role=%s", RoleLeader)
	}

	store := NewStore(*capacity)

	// Only a leader replicates. NewReplicator returns nil for an empty
	// address list, so a leader started without -followers behaves exactly
	// like the single node from Milestone 3.
	var replicator *Replicator
	if nodeRole == RoleLeader {
		replicator = NewReplicator(followers)
		if replicator == nil {
			log.Printf("leader has no followers: writes will not be replicated")
		} else {
			log.Printf("replicating writes to %d follower(s): %s",
				len(followers), strings.Join(followers, ", "))
		}
	}

	server := NewServer(store, nodeRole, replicator)
	addr := fmt.Sprintf(":%d", *port)

	log.Printf("starting %s node", nodeRole)
	if err := server.ListenAndServe(addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// parseFollowers splits the -followers flag into addresses, tolerating
// spaces after the commas and ignoring empty entries (so a trailing comma
// is harmless).
func parseFollowers(value string) []string {
	var addrs []string
	for _, addr := range strings.Split(value, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}
