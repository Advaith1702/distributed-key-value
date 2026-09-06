package main

import (
	"flag"
	"log"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000",
		"this node's address (host:port): it listens here and identifies itself by this in the cluster")
	clusterList := flag.String("cluster", "",
		"comma-separated addresses of every node in the cluster, including this one; empty means a single-node cluster")
	capacity := flag.Int("capacity", 1000,
		"maximum number of keys the store holds before LRU eviction")
	flag.Parse()

	// Elections finish in a few hundred milliseconds, so second-precision
	// timestamps would collapse a whole election into one instant.
	// Microseconds make the ordering of transitions readable across
	// terminals.
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	if strings.TrimSpace(*addr) == "" {
		log.Fatalf("-addr is required")
	}

	peers, err := clusterPeers(*addr, *clusterList)
	if err != nil {
		log.Fatalf("%v", err)
	}

	store := NewStore(*capacity)

	// Every node gets a replicator for its peers, not just whichever node
	// happens to be leading now: roles change with every election, so any
	// node may need to start replicating at any moment. It stays unused
	// until this node actually wins one.
	replicator := NewReplicator(peers)

	node := NewNode(*addr, peers)
	server := NewServer(store, node, replicator)

	if len(peers) == 0 {
		log.Printf("single-node cluster: this node will elect itself and has nobody to replicate to")
	} else {
		log.Printf("cluster of %d nodes; peers: %s", len(peers)+1, strings.Join(peers, ", "))
	}

	// Start the election machinery before serving, so the node is already
	// counting down its first election timeout when peers begin talking
	// to it.
	node.Start()

	if err := server.ListenAndServe(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// clusterPeers splits the -cluster flag into addresses and returns every
// node except this one. An empty list means a single-node cluster.
func clusterPeers(addr string, clusterList string) ([]string, error) {
	addr = strings.TrimSpace(addr)

	var peers []string
	found := false

	for _, member := range strings.Split(clusterList, ",") {
		member = strings.TrimSpace(member)
		if member == "" {
			// Tolerate stray or trailing commas.
			continue
		}

		if member == addr {
			// This node is in its own cluster list, which is expected --
			// the same list can be handed to every node unchanged.
			found = true
			continue
		}
		peers = append(peers, member)
	}

	// A node that is missing from its own cluster list would still run,
	// but it would never be counted in anyone's majority and its own
	// majority arithmetic would be off by one. That is a startup mistake
	// worth refusing rather than debugging later from election logs.
	if len(peers) > 0 && !found {
		return nil, &clusterConfigError{addr: addr, cluster: clusterList}
	}

	return peers, nil
}

// clusterConfigError reports a -cluster list that does not contain this
// node's own -addr.
type clusterConfigError struct {
	addr    string
	cluster string
}

func (e *clusterConfigError) Error() string {
	return "this node's -addr " + e.addr + " is not in -cluster " + e.cluster +
		"; every node must appear in the cluster list, including itself"
}
