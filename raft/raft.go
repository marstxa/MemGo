package raft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type PersistanceState struct {
	CurrentTerm       int
	VotedFor          string
	Snapshot          []byte // the actual kv map serialised JSON
	LastIncludedIndex int
	LastIncludedTerm  int
	Log               []LogEntry // only hold entires AFTER the LastIncludedIndex
}

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

	ApplyCh chan []byte // use to send committed commands to main application
}

func NewRaftNode(id string, peers []string, ch chan []byte) *RaftNode {
	rn := &RaftNode{
		id:         id,
		peers:      peers,
		state:      FOLLOWER,
		votedFor:   "",
		log:        []LogEntry{{Index: 0, Term: 0}},
		resetTimer: make(chan struct{}, 1),
		ApplyCh:    ch,
	}

	// load state from disk before starting
	rn.restore()

	// start election VIVA LA DEMOCRACIA
	go rn.runElectionTimer()

	return rn
}

func (rn *RaftNode) applyCommited() {
	var entriesToApply []LogEntry

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entriesToApply = append(entriesToApply, rn.log[rn.lastApplied])
	}

	if len(entriesToApply) > 0 {
		go func(entries []LogEntry) {
			for _, entry := range entries {
				// Ships the raw RESP bytes out of the Raft Engine
				rn.ApplyCh <- entry.Command
			}
		}(entriesToApply)
	}
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
	rn.persist()
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
		rn.persist()
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
	lastLogIndex := len(rn.log) - 1

	for _, peer := range rn.peers {
		rn.nextIndex[peer] = lastLogIndex + 1
		rn.matchIndex[peer] = 0
	}
	go rn.sendHeartbeats()

	// raft "no-op" force commitIndex to advance on reboot
	go func() {
		// give 50ms to establish heartbeats
		time.Sleep(50 * time.Millisecond)

		// submit raw RESP "PING" command
		// this forces a new log entry in the current term, unlocking backlogging
		rn.Submit([]byte("*1\r\n$4\r\nPING\r\n"))
	}()
}

func (rn *RaftNode) stepDown(newTerm int) {
	rn.currentTerm = newTerm
	rn.state = FOLLOWER
	rn.votedFor = ""
	rn.persist()
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

				if nextIdx < 1 {
					nextIdx = 1
					rn.nextIndex[peerAddr] = 1
				}

				if nextIdx > len(rn.log) {
					nextIdx = len(rn.log)
					rn.nextIndex[peerAddr] = nextIdx
				}

				prevLogIndex := nextIdx - 1
				prevLogTerm := rn.log[prevLogIndex].Term

				// gra and make a copy of all entries from nextIdx to the end of the log
				var entries []LogEntry
				if nextIdx < len(rn.log) {
					entries = make([]LogEntry, len(rn.log)-nextIdx)
					copy(entries, rn.log[nextIdx:])
				}
				rn.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderID:     leaderID,
					PrevLogIndex: prevLogIndex,
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
						return
					}

					// ONLY process reply if we are still the leader
					if rn.state == LEADER && rn.currentTerm == term {
						if reply.Success {
							rn.nextIndex[peerAddr] = nextIdx + len(entries)
							rn.matchIndex[peerAddr] = rn.nextIndex[peerAddr] - 1

							rn.advanceCommitIndex()
						} else {
							// decrement index so we send older data to next heartbeat
							if rn.nextIndex[peerAddr] > 1 {
								rn.nextIndex[peerAddr]--
							}
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

	// handle log entries

	if args.PrevLogIndex > len(rn.log)-1 {
		return nil
	}

	if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	// if we reach here that means the logs match
	// we trucate our lugs to remove any uncommited trash from old leaders
	rn.log = rn.log[:args.PrevLogIndex+1]
	rn.log = append(rn.log, args.Entries...)
	rn.persist()

	// update commit index
	if args.LeaderCommit > rn.commitIndex {
		lastNewEntryIndex := len(rn.log) - 1

		if args.LeaderCommit < lastNewEntryIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewEntryIndex
		}

		fmt.Printf("Node %s advance commitIndex to %d\n", rn.id, rn.commitIndex)
		rn.applyCommited()
	}
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
	rn.persist()

	fmt.Printf("Leader %s append entry %d (Term %d) to its own local log\n", rn.id, index, term)

	return true, index
}

// checks if the majority of followers have replicated a log entry
// and if so, safely updates the leader's commit index
// MUST be called while rn.mu is locked!
func (rn *RaftNode) advanceCommitIndex() {
	majority := (len(rn.peers)+1)/2 + 1

	// start from newest entry going backwards down to current commitIndex
	for n := len(rn.log) - 1; n > rn.commitIndex; n-- {

		if rn.log[n].Term != rn.currentTerm {
			continue
		}

		count := 1 // servers that have this entry
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= n {
				count++
			}
		}

		if count >= majority {
			rn.commitIndex = n
			fmt.Printf("Leader %s advanced commitIndex to %d (Data is now safe)", rn.id, rn.commitIndex)

			rn.applyCommited()

			break
		}

	}
}

// persist saves the node critical state to the disk
func (rn *RaftNode) persist() {
	state := PersistanceState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
		Log:         rn.log,
	}

	// convert the state to a JSON byte slice
	data, err := json.Marshal(state)
	if err != nil {
		fmt.Printf("Failed to marshal state for persistance: %v\n", err)
		return
	}

	filename := fmt.Sprintf("raft-state-%s.json", rn.id)

	// write to disk
	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		fmt.Printf("Failed to write state to disk: %v\n", err)
		return
	}
}

// restore loads the state frmom disk when the server boots
func (rn *RaftNode) restore() {
	filename := fmt.Sprintf("raft-state-%s.json", rn.id)

	data, err := os.ReadFile(filename)
	if err != nil {
		// brand new server
		return
	}

	var state PersistanceState
	err = json.Unmarshal(data, &state)
	if err != nil {
		fmt.Printf("Failed to unmarshal restored state: %v\n", err)
		return
	}

	// restore critical variables
	rn.currentTerm = state.CurrentTerm
	rn.votedFor = state.VotedFor
	rn.log = state.Log

	fmt.Printf("Node %s restored from disk! Term %d, Log Lenght: %d\n", rn.id, rn.currentTerm, len(rn.log))
}

// TODO: implement
// func (rn *RaftNode) getRealIndex(raftIndex int) int {
// 	return raftIndex - rn.LastIncludedIndex
// }
