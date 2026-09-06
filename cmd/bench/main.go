// Command bench measures throughput and latency of a running kvstore
// cluster by pointing N concurrent clients at whichever node is currently
// the leader.
//
// Usage, from the module root:
//
//	go run ./cmd/bench -cluster 127.0.0.1:9601,127.0.0.1:9602 -clients 50 -duration 10s
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

type config struct {
	clients   int
	duration  time.Duration
	readRatio float64
	keySpace  int
	valueSize int
	pipeline  int
}

// sample is one completed operation.
type sample struct {
	op      string
	latency time.Duration
}

// pending is a request that has been written but not yet answered.
type pending struct {
	op   string
	sent time.Time
}

func main() {
	cluster := flag.String("cluster", "127.0.0.1:9000",
		"comma-separated node addresses; the current leader is found automatically")
	clients := flag.Int("clients", 50, "number of concurrent clients")
	duration := flag.Duration("duration", 10*time.Second, "how long to run, e.g. 10s or 1m")
	readRatio := flag.Float64("read-ratio", 0.5, "fraction of operations that are GETs (0 = all SET, 1 = all GET)")
	keySpace := flag.Int("keys", 1000, "number of distinct keys to spread operations over")
	valueSize := flag.Int("value-size", 32, "size of the value written by SET, in bytes")
	pipeline := flag.Int("pipeline", 1,
		"requests in flight per client: 1 is strict request/response (sync-style), higher pipelines them (async-style)")
	flag.Parse()

	if *clients < 1 || *duration <= 0 || *pipeline < 1 {
		log.Fatalf("-clients and -pipeline must be at least 1, and -duration must be positive")
	}
	if *readRatio < 0 || *readRatio > 1 {
		log.Fatalf("-read-ratio must be between 0 and 1")
	}

	addrs := splitAddrs(*cluster)
	leader, err := findLeader(addrs)
	if err != nil {
		log.Fatalf("%v", err)
	}

	cfg := config{
		clients:   *clients,
		duration:  *duration,
		readRatio: *readRatio,
		keySpace:  *keySpace,
		valueSize: *valueSize,
		pipeline:  *pipeline,
	}

	mode := "strict request/response (sync-style)"
	if cfg.pipeline > 1 {
		mode = fmt.Sprintf("%d requests in flight (async-style)", cfg.pipeline)
	}

	fmt.Printf("kvstore benchmark\n")
	fmt.Printf("  leader        %s\n", leader)
	fmt.Printf("  clients       %d\n", cfg.clients)
	fmt.Printf("  duration      %s\n", cfg.duration)
	fmt.Printf("  read ratio    %.2f\n", cfg.readRatio)
	fmt.Printf("  key space     %d\n", cfg.keySpace)
	fmt.Printf("  pipeline      %s\n\n", mode)
	fmt.Printf("running...\n\n")

	// Every client collects into its own slice, so no lock is needed on
	// the hot path and the measurement does not perturb what it measures.
	collected := make([][]sample, cfg.clients)
	failures := make([]error, cfg.clients)

	deadline := time.Now().Add(cfg.duration)
	started := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < cfg.clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			collected[id], failures[id] = runClient(id, leader, cfg, deadline)
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(started)
	report(collected, failures, elapsed)
}

// runClient drives one connection until the deadline and returns the
// latency of every operation it completed.
func runClient(id int, addr string, cfg config, deadline time.Time) ([]sample, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	// inflight both carries send timestamps to the reader and, through its
	// capacity, limits how many requests may be outstanding at once. A
	// capacity of 1 makes the client strictly synchronous.
	inflight := make(chan pending, cfg.pipeline)

	var samples []sample
	var readErr error
	readerDone := make(chan struct{})

	// The protocol answers one line per command in order, so responses can
	// be matched to requests by position -- no request IDs needed.
	go func() {
		defer close(readerDone)
		for p := range inflight {
			if _, err := reader.ReadString('\n'); err != nil {
				readErr = err
				return
			}
			samples = append(samples, sample{op: p.op, latency: time.Since(p.sent)})
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	value := strings.Repeat("x", cfg.valueSize)

	var writeErr error
	for time.Now().Before(deadline) {
		op, command := nextCommand(rng, cfg, value)

		inflight <- pending{op: op, sent: time.Now()}
		if _, err := writer.WriteString(command); err != nil {
			writeErr = err
			break
		}
		if err := writer.Flush(); err != nil {
			writeErr = err
			break
		}
	}

	close(inflight)
	<-readerDone

	if writeErr != nil {
		return samples, writeErr
	}
	return samples, readErr
}

// nextCommand picks the next operation for a client to send.
func nextCommand(rng *rand.Rand, cfg config, value string) (op string, command string) {
	key := fmt.Sprintf("bench:%d", rng.Intn(cfg.keySpace))

	if rng.Float64() < cfg.readRatio {
		return "GET", "GET " + key + "\n"
	}
	return "SET", "SET " + key + " " + value + "\n"
}

// report prints the totals and the latency distribution.
func report(collected [][]sample, failures []error, elapsed time.Duration) {
	var all, sets, gets []time.Duration

	for _, clientSamples := range collected {
		for _, s := range clientSamples {
			all = append(all, s.latency)
			if s.op == "SET" {
				sets = append(sets, s.latency)
			} else {
				gets = append(gets, s.latency)
			}
		}
	}

	errored := 0
	var firstErr error
	for _, err := range failures {
		if err != nil {
			errored++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	total := len(all)
	fmt.Printf("results\n")
	fmt.Printf("  total ops     %d\n", total)
	if elapsed > 0 {
		fmt.Printf("  throughput    %.0f ops/sec\n", float64(total)/elapsed.Seconds())
	}
	fmt.Printf("  elapsed       %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  clients with errors  %d\n", errored)
	if firstErr != nil {
		fmt.Printf("  first error   %v\n", firstErr)
	}

	if total == 0 {
		fmt.Printf("\nno operations completed\n")
		return
	}

	fmt.Printf("\nlatency (ms)        p50       p99       max\n")
	printLatency("all", all)
	printLatency("SET", sets)
	printLatency("GET", gets)

	// The comparison worth reading off these numbers: SET is answered by
	// the leader as soon as its own Store is updated, because replication
	// to followers is asynchronous. No follower round trip is on this
	// path, so SET latency sits alongside GET latency instead of above it.
	// Synchronous replication would add at least one leader-to-follower
	// round trip to every SET line here, and would make SET latency
	// dependent on the slowest follower rather than on the leader alone.
	fmt.Printf("\nSET does not include a follower round trip: replication is\n")
	fmt.Printf("asynchronous, so the leader answers before followers hold the\n")
	fmt.Printf("write. Synchronous replication would push the SET row above\n")
	fmt.Printf("the GET row by the cost of the slowest follower.\n")
}

func printLatency(label string, values []time.Duration) {
	if len(values) == 0 {
		fmt.Printf("  %-14s %9s %9s %9s\n", label, "-", "-", "-")
		return
	}

	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	fmt.Printf("  %-14s %9.3f %9.3f %9.3f\n",
		label,
		millis(percentile(values, 50)),
		millis(percentile(values, 99)),
		millis(values[len(values)-1]),
	)
}

// percentile returns the p-th percentile of an already sorted slice, using
// the nearest-rank method.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// findLeader probes each address with a read that only a leader will
// answer. A follower or candidate rejects client commands outright, so the
// one node that returns NOT_FOUND is the leader. The probe key is never
// written, so this does not disturb the data.
func findLeader(addrs []string) (string, error) {
	for _, addr := range addrs {
		response, err := probe(addr, "GET __bench_leader_probe__")
		if err != nil {
			continue
		}
		if response == "NOT_FOUND" {
			return addr, nil
		}
	}
	return "", fmt.Errorf("no leader found among %s (is the cluster running and has it elected one?)",
		strings.Join(addrs, ", "))
}

// probe sends one command on a throwaway connection and returns the reply.
func probe(addr string, command string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return "", err
	}
	if _, err := fmt.Fprintf(conn, "%s\n", command); err != nil {
		return "", err
	}

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func splitAddrs(list string) []string {
	var addrs []string
	for _, addr := range strings.Split(list, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}
