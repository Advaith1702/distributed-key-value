package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// Role is a node's current standing in the cluster. Unlike the previous
// milestone, where the role was fixed by a startup flag, a role is now
// election state: every node starts as a follower and any node can end up
// leading a term.
type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

const (
	// heartbeatInterval is how often a leader tells the rest of the
	// cluster it is still alive. It has to be comfortably shorter than the
	// smallest election timeout, or followers would time out and start
	// elections against a leader that is perfectly healthy. 50ms against a
	// 150ms floor gives roughly three chances to land a heartbeat before
	// anyone gives up on the leader.
	heartbeatInterval = 50 * time.Millisecond

	// The election timeout is randomized per node, and re-randomized on
	// every reset. That randomness is what breaks ties: if every node used
	// the same timeout they would all become candidates at the same
	// instant, split the vote so nobody reached a majority, and repeat --
	// possibly forever. Staggering the timeouts means one node almost
	// always wakes up first and wins before the others even start.
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond

	// rpcTimeout bounds a single heartbeat or vote request. It is kept
	// well under electionTimeoutMin so that collecting votes from a dead
	// peer cannot itself take longer than the election it belongs to.
	rpcTimeout = 100 * time.Millisecond
)

// Node holds one node's election state and drives the two timers that make
// elections happen: a leader's heartbeat ticker and everyone else's
// election timeout.
//
// This is a deliberately simplified Raft: there is no replicated log, so
// there is no log-completeness check when voting. Real Raft refuses to vote
// for a candidate whose log is behind the voter's, which is what guarantees
// a new leader already holds every committed entry. Here any node may win
// any election, so a node that missed writes can be elected and the cluster
// will simply be missing them -- acceptable for learning how elections
// work, not acceptable for keeping data safe.
type Node struct {
	addr  string
	peers []*peer

	// mu guards every field below it. All of them change together during a
	// transition, so they share one lock rather than one each.
	mu       sync.Mutex
	term     int
	role     Role
	votedFor string

	// resetElection carries "a leader or candidate contacted us, restart
	// the countdown" from request handlers to the election timer
	// goroutine. It is buffered so a handler never blocks on it.
	resetElection chan struct{}
}

// NewNode creates a node that knows about every peer in the cluster.
// peerAddrs must not include the node's own address.
func NewNode(addr string, peerAddrs []string) *Node {
	n := &Node{
		addr: addr,
		// Every node starts as a follower at term 0 and waits. Nobody
		// starts as leader: the first election decides that.
		term:          0,
		role:          RoleFollower,
		resetElection: make(chan struct{}, 1),
	}
	for _, peerAddr := range peerAddrs {
		n.peers = append(n.peers, &peer{addr: peerAddr})
	}
	return n
}

// Start launches the node's two background loops. It returns immediately.
func (n *Node) Start() {
	go n.runElectionTimer()
	go n.runHeartbeats()
}

// Role reports the node's current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// clusterSize counts every node including this one.
func (n *Node) clusterSize() int {
	return len(n.peers) + 1
}

// majority is the smallest number of votes that wins an election. With 3
// nodes it is 2, with 5 it is 3 -- always more than half, so two different
// nodes can never both win the same term.
func (n *Node) majority() int {
	return n.clusterSize()/2 + 1
}

// logf prints a state transition with a timestamp and the node's identity,
// so several nodes' logs can be read side by side across terminals.
// Callers must hold n.mu, since the term and role are part of the message.
func (n *Node) logf(format string, args ...any) {
	log.Printf("[%s term %d %s] %s", n.addr, n.term, n.role, fmt.Sprintf(format, args...))
}

// runElectionTimer waits out a randomized timeout and starts an election if
// nothing reset it first. Each pass picks a fresh random duration.
func (n *Node) runElectionTimer() {
	for {
		timeout := electionTimeoutMin +
			time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))

		select {
		case <-n.resetElection:
			// A leader's heartbeat or a candidate's vote request arrived,
			// so the cluster is alive and this node has no reason to
			// start an election. Loop around for a new random timeout.

		case <-time.After(timeout):
			n.startElection()
		}
	}
}

// runHeartbeats ticks forever and sends heartbeats on the ticks where this
// node happens to be leader. One always-running ticker is simpler to reason
// about than starting and stopping a goroutine on every role change, and
// the cost of a no-op tick is nothing.
func (n *Node) runHeartbeats() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		isLeader := n.role == RoleLeader
		term := n.term
		n.mu.Unlock()

		if !isLeader {
			continue
		}

		// One goroutine per peer, so an unreachable peer delays only its
		// own heartbeat and not the others'.
		for _, p := range n.peers {
			go n.sendHeartbeat(p, term)
		}
	}
}

// sendHeartbeat sends one empty heartbeat to one peer.
func (n *Node) sendHeartbeat(p *peer, term int) {
	response, err := p.callIfIdle(fmt.Sprintf("HEARTBEAT %d %s", term, n.addr))
	if err != nil {
		// Deliberately silent. Heartbeats go out 20 times a second, so
		// logging every failure against a down peer would bury the state
		// transitions this milestone exists to make visible.
		return
	}

	var peerTerm int
	if _, err := fmt.Sscanf(response, "ACK %d", &peerTerm); err == nil {
		n.observeTerm(peerTerm)
	}
}

// startElection runs one full election attempt: become a candidate, ask
// every peer for a vote, and take the leadership if a majority says yes.
func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == RoleLeader {
		// A leader has no election timeout to honour; it keeps its term
		// until it hears about a higher one.
		n.mu.Unlock()
		return
	}

	// Becoming a candidate means claiming a brand new term and voting for
	// yourself in it. Voting for itself is why a single-node cluster can
	// elect itself instantly, and why a candidate only needs majority-1
	// votes from its peers.
	n.term++
	n.role = RoleCandidate
	n.votedFor = n.addr
	term := n.term
	n.logf("election timeout fired, became candidate and voted for itself")
	n.mu.Unlock()

	// Vote counting starts at 1 for the self-vote above.
	var votesMu sync.Mutex
	votes := 1

	var wg sync.WaitGroup
	for _, p := range n.peers {
		wg.Add(1)
		go func(p *peer) {
			defer wg.Done()

			response, err := p.callIfIdle(fmt.Sprintf("REQUESTVOTE %d %s", term, n.addr))
			if err != nil {
				// An unreachable peer is simply a vote this candidate
				// does not get. That is the whole failure model here:
				// silence counts as "no".
				return
			}

			var granted string
			var peerTerm int
			if _, err := fmt.Sscanf(response, "VOTE %s %d", &granted, &peerTerm); err != nil {
				return
			}

			n.observeTerm(peerTerm)
			if granted == "yes" {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
		}(p)
	}

	// Waiting for every peer before counting is a simplification: real
	// Raft declares victory the moment the majority arrives. Since
	// rpcTimeout is shorter than the shortest election timeout, the wait
	// is bounded well inside one election round.
	wg.Wait()

	votesMu.Lock()
	got := votes
	votesMu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	// While votes were being collected this node may have learned about a
	// higher term and stepped down, or moved on to a later election. If
	// so, the result of this one is stale and must be discarded --
	// otherwise a node could become leader for a term it already left.
	if n.role != RoleCandidate || n.term != term {
		return
	}

	if got >= n.majority() {
		n.role = RoleLeader
		n.logf("won election with %d/%d votes, became leader", got, n.clusterSize())
		return
	}

	n.logf("election failed with %d/%d votes (needed %d), retrying after the next timeout",
		got, n.clusterSize(), n.majority())
}

// observeTerm steps down if some other node is already on a higher term.
// Seeing a higher term anywhere is unconditional proof that this node is
// out of date, whatever role it currently thinks it holds.
func (n *Node) observeTerm(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term > n.term {
		n.stepDown(term, "saw a higher term from another node")
	}
}

// stepDown moves the node to a new, higher term as a follower. Callers must
// hold n.mu.
func (n *Node) stepDown(term int, reason string) {
	previous := n.role

	n.term = term
	n.role = RoleFollower
	// The vote belongs to the old term; the new term starts unvoted.
	n.votedFor = ""

	if previous == RoleFollower {
		n.logf("term advanced: %s", reason)
		return
	}
	n.logf("stepped down from %s to follower: %s", previous, reason)
}

// resetElectionTimer restarts the election countdown. It never blocks: if a
// reset is already queued, one more is redundant.
func (n *Node) resetElectionTimer() {
	select {
	case n.resetElection <- struct{}{}:
	default:
	}
}

// HandleHeartbeat processes a heartbeat from a leader and returns the
// response line. An up-to-date heartbeat is what keeps followers from
// starting elections.
func (n *Node) HandleHeartbeat(term int, from string) string {
	n.mu.Lock()
	defer n.mu.Unlock()

	// A heartbeat from an older term comes from a leader that has been
	// deposed and does not know it yet. Answering with the current term
	// tells it to step down, and crucially the election timer is NOT reset
	// -- letting a stale leader hold off elections would keep the cluster
	// leaderless if the real leader then died.
	if term < n.term {
		return fmt.Sprintf("ACK %d", n.term)
	}

	if term > n.term {
		n.stepDown(term, fmt.Sprintf("heartbeat from %s in a higher term", from))
	} else if n.role != RoleFollower {
		// Same term, and someone else is leading it. A candidate loses
		// this election; a leader in the same term should be impossible,
		// since a majority cannot elect two nodes in one term.
		previous := n.role
		n.role = RoleFollower
		n.logf("stepped down from %s to follower: %s is leading this term", previous, from)
	}

	n.resetElectionTimer()
	return fmt.Sprintf("ACK %d", n.term)
}

// HandleRequestVote processes a candidate's request for a vote and returns
// the response line.
func (n *Node) HandleRequestVote(term int, candidate string) string {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Never vote in a term the node has already moved past.
	if term < n.term {
		return fmt.Sprintf("VOTE no %d", n.term)
	}

	if term > n.term {
		n.stepDown(term, fmt.Sprintf("vote request from %s in a higher term", candidate))
	}

	// One vote per term, first come first served. This is what stops two
	// candidates from both collecting a majority in the same term: the
	// votes they need overlap, and no node will give both of them away.
	// Re-granting to the same candidate keeps a retried request harmless.
	granted := n.votedFor == "" || n.votedFor == candidate
	if granted {
		n.votedFor = candidate
		n.logf("voted for %s", candidate)
	}

	// The timer is reset either way: a vote request proves some other node
	// is awake and trying to form a quorum, so piling on with a competing
	// election immediately would only split the vote further.
	n.resetElectionTimer()

	if granted {
		return fmt.Sprintf("VOTE yes %d", n.term)
	}
	return fmt.Sprintf("VOTE no %d", n.term)
}

// peer is this node's client side of the link to one other node, used for
// heartbeats and vote requests. The connection is persistent and redialed
// lazily after an error, the same way replication connections work.
type peer struct {
	addr string

	// mu serializes requests on conn, which is a single connection
	// carrying one request and one response at a time.
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
}

// callIfIdle sends one request and reads one response, but gives up
// immediately if another request to this peer is still in flight. Skipping
// matters because heartbeats are issued every 50ms while a request to a
// dead peer can take up to rpcTimeout: without this, goroutines would pile
// up faster than they drain.
func (p *peer) callIfIdle(request string) (string, error) {
	if !p.mu.TryLock() {
		return "", fmt.Errorf("peer %s: previous request still in flight", p.addr)
	}
	defer p.mu.Unlock()

	response, err := p.send(request)
	if err != nil {
		// Any failure invalidates the connection; drop it so the next
		// request redials.
		p.disconnect()
		return "", err
	}
	return response, nil
}

// send writes one request and reads one response on the peer connection.
func (p *peer) send(request string) (string, error) {
	if p.conn == nil {
		conn, err := net.DialTimeout("tcp", p.addr, rpcTimeout)
		if err != nil {
			return "", fmt.Errorf("dialing %s: %w", p.addr, err)
		}
		p.conn = conn
		p.reader = bufio.NewReader(conn)
	}

	// One deadline covers the write and the read, so a peer that accepts
	// the connection but never answers cannot hang this goroutine.
	if err := p.conn.SetDeadline(time.Now().Add(rpcTimeout)); err != nil {
		return "", err
	}

	if _, err := fmt.Fprintf(p.conn, "%s\n", request); err != nil {
		return "", fmt.Errorf("writing to %s: %w", p.addr, err)
	}

	line, err := p.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("reading from %s: %w", p.addr, err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// disconnect closes the current connection so the next request redials.
func (p *peer) disconnect() {
	if p.conn == nil {
		return
	}
	p.conn.Close()
	p.conn = nil
	p.reader = nil
}
