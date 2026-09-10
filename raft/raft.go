package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	Term        int
	CandidateID string
	// TODO: LastLogIndex, LastLogTerm
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Index   int    // Serial number
	Term    int    // Election term this was created in
	Command []byte // actual raw RESP
}

type RaftNode struct {
	mu sync.Mutex

	// Replica information
	id    string
	peers []string // address of all other nodes
	state State    // follower, candidate or leader

	currentTerm int
	votedFor    string // candidate id that received vote in current term
	log         []LogEntry

	// volatile
	commitIndex int            // index of highest log to be commited
	lastApplied int            // index of highest log to be applied to Store
	nextIndex   map[string]int // for each ppeer index of the next log entry to send
	matchIndex  map[string]int // for each peer index of highest log entry to be replicated

	resetTimer chan struct{} // channels for signalin
}

func NewRaftNode(id string, peers []string) *RaftNode {
	rn := &RaftNode{
		id:       id,
		peers:    peers,
		state:    FOLLOWER,
		votedFor: "",

		// dummy
		log:        []LogEntry{{Index: 0, Term: 0}},
		resetTimer: make(chan struct{}, 1),
	}
	// start election VIVA LA DEMOCRACIA
	go rn.runElectionTimer()

	return rn
}

func (rn *RaftNode) runElectionTimer() {

	for {
		// pick a random time between 1500ms and 3000ms
		timeout := time.Duration(1500+rand.Intn(1500)) * time.Millisecond

		select {
		case <-rn.resetTimer:
			// we received a hearbeat from a leader, loop restarts
			// fmt.Println("Timer reset by leader heartbeat")
		case <-time.After(timeout):
			rn.mu.Lock()
			isLeader := rn.state == LEADER
			rn.mu.Unlock()

			if !isLeader {
				rn.StartElection()
			}
		}
	}
}

func (rn *RaftNode) StartElection() {
	rn.mu.Lock()

	rn.state = CANDIDATE
	rn.currentTerm++

	rn.votedFor = rn.id
	currentTerm := rn.currentTerm

	fmt.Printf("Node %s starting election for Term %d\n", rn.id, rn.currentTerm)

	rn.mu.Unlock()

	rn.requestVotes(currentTerm)
}

func (rn *RaftNode) requestVotes(term int) {
	fmt.Println("Asking peers for votes...")

	rn.mu.Lock()
	votesReceived := 1 // voted for ourselves already

	// Need more than half of the total servers to win
	majority := (len(rn.peers)+1)/2 + 1
	rn.mu.Unlock()

	for _, peer := range rn.peers {
		go func(peerAddr string) {
			args := RequestVoteArgs{
				Term:        term,
				CandidateID: rn.id,
			}

			var reply RequestVoteReply
			err := rn.sendRPC(peerAddr, "RaftNode.RequestVote", args, &reply)

			if err == nil {
				rn.mu.Lock()
				defer rn.mu.Unlock()

				// check if followers told us term is too old
				if reply.Term > rn.currentTerm {
					rn.stepDown(reply.Term)
					return
				}

				if rn.state == CANDIDATE && rn.currentTerm == term && reply.VoteGranted {
					votesReceived++
					fmt.Printf("Node %s got a vote from %s (Total: %d/%d)\n", rn.id, peerAddr, votesReceived, majority)

					if votesReceived >= majority {
						rn.becomeLeader()
					}
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// default reply "no you dont get my vote, work for it bud"
	reply.VoteGranted = false
	reply.Term = rn.currentTerm

	// reject if candidate term older than ours
	if args.Term < rn.currentTerm {
		return nil
	}

	// term is newer, we must update and stepdown
	if args.Term > rn.currentTerm {
		rn.stepDown(args.Term)
		reply.Term = rn.currentTerm
	}

	if rn.votedFor == "" || rn.votedFor == args.CandidateID {

		rn.votedFor = args.CandidateID
		reply.VoteGranted = true

		// MUST reset election timer after we make a vote, so we dont start our own competing election
		// use a non-blocking send so it doesnt freeze if the channel is full
		select {
		case rn.resetTimer <- struct{}{}:
		default:
		}

		fmt.Printf("Node %s voted for %s in Term %d\n", rn.id, args.CandidateID, rn.currentTerm)
	}

	return nil
}

func (rn *RaftNode) becomeLeader() {
	rn.state = LEADER
	fmt.Printf("Node %s WON THE ELECTION! Now Leader for Term %d\n", rn.id, rn.currentTerm)

	rn.nextIndex = make(map[string]int)
	rn.matchIndex = make(map[string]int)

	// raft rule: nextIndex is initialised to leaders last log index + 1
	lastLogIndex := len(rn.log)

	for _, peer := range rn.peers {
		rn.nextIndex[peer] = lastLogIndex + 1
		rn.matchIndex[peer] = 0
	}
	go rn.sendHeartbeats()
}

func (rn *RaftNode) stepDown(newTerm int) {
	rn.currentTerm = newTerm
	rn.state = FOLLOWER
	rn.votedFor = ""
	fmt.Printf("Node %s stepping down to Follower for Term %d\n", rn.id, newTerm)
}

func (rn *RaftNode) sendHeartbeats() {
	for {
		rn.mu.Lock()

		if rn.state != LEADER {
			rn.mu.Unlock()
			return
		}
		term := rn.currentTerm
		leaderID := rn.id
		leaderCommit := rn.commitIndex
		rn.mu.Unlock()

		// Send a heartbeat to every peer
		for _, peer := range rn.peers {
			go func(peerAddr string) {
				rn.mu.Lock()

				nextIdx := rn.nextIndex[peerAddr]
				prevLogIndex := nextIdx - 1
				prevLogTerm := rn.log[prevLogIndex].Term

				// gra and make a copy of all entries from nextIdx to the end of the log
				entries := make([]LogEntry, len(rn.log[nextIdx:]))
				copy(entries, rn.log[nextIdx:])
				rn.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderID:     leaderID,
					PrevLogIndex: prevLogTerm,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				var reply AppendEntriesReply

				err := rn.sendRPC(peerAddr, "RaftNode.AppendEntries", args, &reply)
				if err == nil {
					rn.mu.Lock()
					defer rn.mu.Unlock()

					if reply.Term > rn.currentTerm {
						rn.stepDown(reply.Term)
					}

					// ONLY process reply if we are still the leader
					if rn.state == LEADER && rn.currentTerm == term {
						if reply.Success {
							rn.nextIndex[peerAddr] = nextIdx + len(entries)
							rn.matchIndex[peerAddr] = rn.nextIndex[peerAddr] - 1
						} else {
							// decrement index so we send older data to next heartbeat
							rn.nextIndex[peerAddr]--
							fmt.Printf("Follower %s rejected log; backtracking nextIndex to %d\n", peerAddr, rn.nextIndex[peerAddr])
						}
					}
				}
			}(peer)
		}

		// wait before sending the next round of hearbeats
		time.Sleep(150 * time.Millisecond)
	}
}

func (rn *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Success = false
	reply.Term = rn.currentTerm

	// reject if older term
	if args.Term < rn.currentTerm {
		return nil
	}

	// if we are a candidate, older leader and see a valid hearbeat, we lost the election so stepdown
	if args.Term > rn.currentTerm || rn.state == CANDIDATE {
		rn.stepDown(args.Term)
		reply.Term = rn.currentTerm

	}

	select {
	case rn.resetTimer <- struct{}{}:
	default:
	}

	// TODO: add log later here

	reply.Success = true
	return nil
}

// dials a peer, calls a method and unmarshals the response
func (rn *RaftNode) sendRPC(peerAddr, method string, args, reply interface{}) error {

	client, err := rpc.Dial("tcp", peerAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Call(method, args, reply)
}

// opens a TCP port and listen for incomping RPCs from other nodes
func (rn *RaftNode) StartServer() error {
	server := rpc.NewServer()

	err := server.Register(rn)
	if err != nil {
		return err
	}

	// rn.id as our address
	ln, err := net.Listen("tcp", rn.id)
	if err != nil {
		return err
	}

	fmt.Printf("Node %s is up and listening for Raft RPCs...\n", rn.id)

	// run the accept loop in the bg
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue // keep trying
			}

			// hand to the RPC server
			go server.ServeConn(conn)
		}
	}()

	return nil
}

func (rn *RaftNode) Submit(command []byte) (bool, int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != LEADER {
		return false, -1
	}

	index := len(rn.log)
	term := rn.currentTerm

	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	// append own log
	rn.log = append(rn.log, entry)

	fmt.Printf("Leader %s append entry %d (Term %d) to its own local log\n", rn.id, index, term)

	return true, index
}
