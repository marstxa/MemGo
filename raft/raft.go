package raft

import (
	"fmt"
	"math/rand"
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
	Term     int
	LeaderID string
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

	resetTimer chan struct{} // channels for signaling

	// TODO: nextIndex, matchIndex
}

func NewRaftNode(id string, peers []string) *RaftNode {
	rn := &RaftNode{
		id:         id,
		peers:      peers,
		state:      FOLLOWER,
		votedFor:   "",
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
			fmt.Println("Timer reset by leader heartbeat")

		case <-time.After(timeout):
			rn.StartElection()
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

	rn.mu.Lock()

	// TODO: ask all peers for their votes
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

	// rn.sendHeartbeats()

}

func (rn *RaftNode) stepDown(newTerm int) {
	rn.currentTerm = newTerm
	rn.state = FOLLOWER
	rn.votedFor = ""
	fmt.Printf("Node %s stepping down to Follower for Term %d\n", rn.id, newTerm)
}
