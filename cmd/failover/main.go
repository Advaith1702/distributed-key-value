// Command failover is a repeatable durability demonstration. It starts a
// 3-node cluster, streams SET commands at the leader, kills the leader
// partway through, waits for a new one to be elected, and then checks
// whether every write the old leader acknowledged is still readable.
//
// Usage, from the module root:
//
//	go run ./cmd/failover
//	go run ./cmd/failover -duration 8s -kill-after 4s
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type node struct {
	addr    string
	cmd     *exec.Cmd
	logPath string
}

func main() {
	os.Exit(run())
}

// run holds the whole test so that cleanup actually happens. Calling
// os.Exit directly from here would skip every deferred function, including
// the one that stops the cluster -- and since a FAIL is the expected
// outcome of this test, that would leak three node processes on every run
// and leave their ports occupied for the next one.
func run() int {
	nodeCount := flag.Int("nodes", 3, "number of nodes in the cluster")
	basePort := flag.Int("base-port", 9601, "first port to use; nodes take consecutive ports from here")
	duration := flag.Duration("duration", 6*time.Second, "how long the write phase runs at most")
	killAfter := flag.Duration("kill-after", 3*time.Second, "how far into the write phase to kill the leader")
	valueSize := flag.Int("value-size", 32, "size of the value written by each SET, in bytes")
	// This must stay comfortably above the number of writes a run
	// produces. The nodes' own default capacity is 1000, and a few seconds
	// of writes is tens of thousands of distinct keys -- at that default,
	// LRU eviction alone would discard almost everything and the test
	// would blame replication for losses that were really evictions.
	capacity := flag.Int("capacity", 1_000_000,
		"per-node key capacity; must exceed the number of writes, or LRU eviction confounds the result")
	keepLogs := flag.Bool("keep-logs", false, "leave the node log files on disk after the run")
	flag.Parse()

	if *nodeCount < 3 {
		log.Fatalf("-nodes must be at least 3, so that killing the leader leaves a majority behind")
	}
	if *killAfter >= *duration {
		log.Fatalf("-kill-after (%s) must be less than -duration (%s)", *killAfter, *duration)
	}

	workDir, err := os.MkdirTemp("", "kvstore-failover-")
	if err != nil {
		log.Fatalf("creating work directory: %v", err)
	}
	if !*keepLogs {
		defer os.RemoveAll(workDir)
	}

	binary, err := buildServer(workDir)
	if err != nil {
		fmt.Printf("building the server: %v\n", err)
		return 1
	}

	addrs := make([]string, *nodeCount)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", *basePort+i)
	}

	fmt.Printf("failover test\n")
	fmt.Printf("  cluster       %s\n", strings.Join(addrs, ", "))
	fmt.Printf("  capacity      %d keys per node\n", *capacity)

	nodes, err := startCluster(binary, addrs, workDir, *capacity)
	defer stopAll(nodes)
	if err != nil {
		fmt.Printf("starting the cluster: %v\n", err)
		return 1
	}

	leader, err := waitForLeader(addrs, "", 10*time.Second)
	if err != nil {
		fmt.Printf("waiting for the first leader: %v\n", err)
		return 1
	}
	fmt.Printf("  first leader  %s\n\n", leader)

	// Write phase. The killer fires partway through; the writer stops as
	// soon as its connection to the dying leader breaks.
	fmt.Printf("streaming SET commands at %s, killing it after %s...\n", leader, *killAfter)

	killed := make(chan struct{})
	go func() {
		time.Sleep(*killAfter)
		if err := kill(nodes, leader); err != nil {
			log.Printf("killing the leader: %v", err)
		}
		close(killed)
	}()

	acked, writeErr := streamWrites(leader, time.Now().Add(*duration), *valueSize)
	<-killed

	fmt.Printf("  writes acknowledged before the kill  %d\n", len(acked))
	if writeErr != nil {
		fmt.Printf("  write stream ended with              %v\n", writeErr)
	}

	if len(acked) == 0 {
		fmt.Printf("\nno writes were acknowledged, so there is nothing to verify\n")
		return 1
	}

	// Guard the premise of the whole test. If the run wrote more distinct
	// keys than a node can hold, LRU eviction has already discarded some
	// of them on every node, and a missing key would say nothing about
	// replication or failover.
	if len(acked) > *capacity {
		fmt.Printf("\nthis run wrote %d keys into a %d-key store, so LRU eviction\n", len(acked), *capacity)
		fmt.Printf("would discard writes no matter how the failover went; re-run\n")
		fmt.Printf("with a larger -capacity or a shorter -duration\n")
		return 1
	}

	// A new leader can only come from the survivors.
	newLeader, err := waitForLeader(addrs, leader, 15*time.Second)
	if err != nil {
		fmt.Printf("\nFAIL: no new leader was elected: %v\n", err)
		return 1
	}
	fmt.Printf("  new leader                           %s\n", newLeader)

	for _, line := range electionLines(nodes, newLeader) {
		fmt.Printf("    %s\n", line)
	}

	// Verification phase.
	fmt.Printf("\nverifying every acknowledged write against the new leader...\n")
	present, missing, err := verify(newLeader, acked, *valueSize)
	if err != nil {
		fmt.Printf("verifying: %v\n", err)
		return 1
	}

	fmt.Printf("  checked        %d\n", len(acked))
	fmt.Printf("  present        %d\n", present)
	fmt.Printf("  missing        %d\n", len(missing))

	if len(missing) == 0 {
		fmt.Printf("\nPASS: all %d acknowledged writes survived the failover\n", len(acked))
		return 0
	}

	fmt.Printf("\nFAIL: %d of %d acknowledged writes were lost (%.2f%%)\n",
		len(missing), len(acked), 100*float64(len(missing))/float64(len(acked)))
	fmt.Printf("  first missing keys: %s\n", strings.Join(firstN(missing, 5), ", "))
	fmt.Printf("\nThis is the documented consequence of asynchronous replication:\n")
	fmt.Printf("the leader answers OK once the write is in its own Store, before\n")
	fmt.Printf("any follower holds it. Writes still queued or in flight when the\n")
	fmt.Printf("leader died existed nowhere else, so the new leader never had\n")
	fmt.Printf("them. Nothing re-sends them, and because there is no replicated\n")
	fmt.Printf("log, an election cannot prefer a more up-to-date node.\n")
	return 1
}

// buildServer compiles the node binary from the module root into workDir.
func buildServer(workDir string) (string, error) {
	binary := filepath.Join(workDir, "kvstore-node.exe")

	cmd := exec.Command("go", "build", "-o", binary, ".")
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("%v: %s", err, output)
	}
	return binary, nil
}

// startCluster launches every node, each writing its log to workDir.
func startCluster(binary string, addrs []string, workDir string, capacity int) ([]*node, error) {
	cluster := strings.Join(addrs, ",")

	var nodes []*node
	for _, addr := range addrs {
		logPath := filepath.Join(workDir, strings.ReplaceAll(addr, ":", "_")+".log")
		logFile, err := os.Create(logPath)
		if err != nil {
			return nodes, err
		}

		cmd := exec.Command(binary, "-addr", addr, "-cluster", cluster,
			"-capacity", strconv.Itoa(capacity))
		cmd.Stdout = logFile
		cmd.Stderr = logFile

		if err := cmd.Start(); err != nil {
			logFile.Close()
			return nodes, err
		}
		nodes = append(nodes, &node{addr: addr, cmd: cmd, logPath: logPath})
	}
	return nodes, nil
}

func stopAll(nodes []*node) {
	for _, n := range nodes {
		terminate(n.cmd)
	}
}

// kill terminates the node listening on addr.
func kill(nodes []*node, addr string) error {
	for _, n := range nodes {
		if n.addr != addr {
			continue
		}
		return terminate(n.cmd)
	}
	return fmt.Errorf("no node with address %s", addr)
}

// terminate stops a child process and waits for it to go away.
//
// It does not simply call Process.Kill, because on Windows that can come
// back with "TerminateProcess: Access is denied" even for a process this
// program started -- which silently turns the whole test into a no-op: the
// leader keeps running, no election happens, and the run reports a
// failure that never had anything to do with the cluster. taskkill is
// granted where the direct call is refused, so it stands in as a fallback.
func terminate(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("process is not running")
	}

	err := cmd.Process.Kill()
	if err != nil && runtime.GOOS == "windows" {
		pid := strconv.Itoa(cmd.Process.Pid)
		output, taskkillErr := exec.Command("taskkill", "/F", "/T", "/PID", pid).CombinedOutput()
		if taskkillErr != nil {
			return fmt.Errorf("%v; taskkill also failed: %v: %s", err, taskkillErr, output)
		}
		err = nil
	}

	// Reap the process so it does not linger as a zombie. The wait status
	// is uninteresting: the process was just killed on purpose.
	cmd.Wait()
	return err
}

// streamWrites sends SET commands as fast as the leader will answer them,
// recording the key of every write that was acknowledged. It stops at the
// deadline or when the connection breaks, whichever comes first.
//
// Recording happens only after OK is read back, so every key in the result
// is one the leader genuinely acknowledged to a client.
func streamWrites(leader string, deadline time.Time, valueSize int) ([]string, error) {
	conn, err := net.Dial("tcp", leader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	value := strings.Repeat("v", valueSize)

	var acked []string
	for i := 0; time.Now().Before(deadline); i++ {
		key := fmt.Sprintf("failover:%d", i)

		if _, err := fmt.Fprintf(conn, "SET %s %s\n", key, value); err != nil {
			return acked, err
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return acked, err
		}
		if strings.TrimRight(line, "\r\n") != "OK" {
			return acked, fmt.Errorf("unexpected response %q", strings.TrimRight(line, "\r\n"))
		}

		acked = append(acked, key)
	}
	return acked, nil
}

// verify reads every acknowledged key back from the new leader.
func verify(leader string, keys []string, valueSize int) (present int, missing []string, err error) {
	conn, err := net.Dial("tcp", leader)
	if err != nil {
		return 0, nil, err
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	want := strings.Repeat("v", valueSize)

	for _, key := range keys {
		if _, err := fmt.Fprintf(conn, "GET %s\n", key); err != nil {
			return present, missing, err
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return present, missing, err
		}

		if strings.TrimRight(line, "\r\n") == want {
			present++
			continue
		}
		missing = append(missing, key)
	}
	return present, missing, nil
}

// waitForLeader polls until some node other than exclude answers as leader.
func waitForLeader(addrs []string, exclude string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			if addr == exclude {
				continue
			}
			// Only a leader serves client reads; followers and candidates
			// answer ERROR unknown command. The probe key is never
			// written, so this does not disturb the data.
			if response, err := probe(addr, "GET __failover_leader_probe__"); err == nil && response == "NOT_FOUND" {
				return addr, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader within %s", timeout)
}

func probe(addr string, command string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
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

// electionLines pulls the election transitions out of a node's log, so the
// run shows the handover rather than just asserting it happened.
func electionLines(nodes []*node, addr string) []string {
	for _, n := range nodes {
		if n.addr != addr {
			continue
		}
		data, err := os.ReadFile(n.logPath)
		if err != nil {
			return nil
		}

		var lines []string
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, "became candidate") || strings.Contains(line, "became leader") {
				lines = append(lines, strings.TrimSpace(line))
			}
		}
		return lines
	}
	return nil
}

func firstN(values []string, n int) []string {
	if len(values) < n {
		return values
	}
	return values[:n]
}
