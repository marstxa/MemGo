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

type InstallSnaphotArgs struct {
	Term              int
	LeaderID          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnaphotReply struct {
	Term int
}

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int

	// Snapshot delivery
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type PersistanceState struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry // only hold entires AFTER the LastIncludedIndex

	Snapshot          []byte // the actual kv map serialised JSON
	LastIncludedIndex int
	LastIncludedTerm  int
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
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
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

	currentLeader string

	// snapshot state
	LastIncludedIndex int
	LastIncludedTerm  int
	snapshot          []byte // keep a copy in memory so persist() can access it

	// volatile
	commitIndex int            // index of highest log to be commited
	lastApplied int            // index of highest log to be applied to Store
	nextIndex   map[string]int // for each ppeer index of the next log entry to send
	matchIndex  map[string]int // for each peer index of highest log entry to be replicated

	resetTimer chan struct{} // channels for signalin

	ApplyCh chan ApplyMsg // use to send committed commands to main application
}

func NewRaftNode(id string, peers []string, ch chan ApplyMsg) *RaftNode {
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
	var msgs []ApplyMsg

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		realIndex := rn.getRealIndex(rn.lastApplied)
		msgs = append(msgs, ApplyMsg{
			CommandValid: true,
			Command:      rn.log[realIndex].Command,
			CommandIndex: rn.lastApplied,
		})
	}

	if len(msgs) > 0 {
		go func(toSend []ApplyMsg) {
			for _, msg := range msgs {
				rn.ApplyCh <- msg
			}
		}(msgs)
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

	lastIndex := rn.getLastLogIndex()
	realLastIdx := rn.getRealIndex(lastIndex)
	lastTerm := rn.log[realLastIdx].Term

	// Need more than half of the total servers to win
	majority := (len(rn.peers)+1)/2 + 1
	rn.mu.Unlock()

	for _, peer := range rn.peers {
		go func(peerAddr string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rn.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
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

	myLastIndex := rn.getLastLogIndex()
	myRealLastIdx := rn.getRealIndex(myLastIndex)
	myLastTerm := rn.log[myRealLastIdx].Term

	logUpToDate := false
	if args.LastLogTerm > myLastTerm {
		logUpToDate = true
	} else if args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex {
		logUpToDate = true
	}

	if !logUpToDate {
		// reject candidate
		return nil
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

func (rn *RaftNode) GetLeader() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentLeader
}

func (rn *RaftNode) becomeLeader() {
	rn.state = LEADER
	rn.currentLeader = rn.id
	fmt.Printf("Node %s WON THE ELECTION! Now Leader for Term %d\n", rn.id, rn.currentTerm)

	rn.nextIndex = make(map[string]int)
	rn.matchIndex = make(map[string]int)

	// raft rule: nextIndex is initialised to leaders last log index + 1
	lastLogIndex := rn.getLastLogIndex()

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
	if newTerm <= rn.currentTerm && rn.state == FOLLOWER {
		return
	}

	if newTerm < rn.currentTerm {
		return
	}

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
				lastLogIndex := rn.getLastLogIndex()

				// if the follower is so far behind we already deleted the logs they need...
				if nextIdx <= rn.LastIncludedIndex {
					args := InstallSnaphotArgs{
						Term:              term,
						LeaderID:          leaderID,
						LastIncludedIndex: rn.LastIncludedIndex,
						LastIncludedTerm:  rn.LastIncludedTerm,
						Data:              rn.snapshot,
					}
					rn.mu.Unlock()

					go func(targetPeer string, snapArgs InstallSnaphotArgs) {
						var snapReply InstallSnaphotReply
						err := rn.sendRPC(targetPeer, "RaftNode.InstallSnapshot", snapArgs, &snapReply)

						if err == nil {
							rn.mu.Lock()
							defer rn.mu.Unlock()

							if snapReply.Term > rn.currentTerm {
								rn.stepDown(snapReply.Term)
								return
							}

							if rn.state == LEADER && rn.currentTerm == snapArgs.Term {
								rn.nextIndex[targetPeer] = snapArgs.LastIncludedIndex + 1
								rn.matchIndex[targetPeer] = snapArgs.LastIncludedIndex
							}
						}
					}(peerAddr, args)

					return
				}

				// guard rails using global index
				if nextIdx > lastLogIndex+1 {
					nextIdx = lastLogIndex + 1
					rn.nextIndex[peerAddr] = nextIdx
				}

				prevLogIndex := nextIdx - 1

				// translate array index before accessing rn.log
				realPrevIndex := rn.getRealIndex(prevLogIndex)
				prevLogTerm := rn.log[realPrevIndex].Term

				var entries []LogEntry
				if nextIdx <= lastLogIndex {
					realNextIndex := rn.getRealIndex(nextIdx)
					entries = make([]LogEntry, len(rn.log)-realNextIndex)
					copy(entries, rn.log[realNextIndex:])
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

					if reply.Term > rn.currentTerm {
						rn.stepDown(reply.Term)
						return
					}

					defer rn.mu.Unlock()

					// ONLY process reply if we are still the leader
					if rn.state == LEADER && rn.currentTerm == term {
						if reply.Success {
							rn.nextIndex[peerAddr] = nextIdx + len(entries)
							rn.matchIndex[peerAddr] = rn.nextIndex[peerAddr] - 1
							rn.advanceCommitIndex()
						} else {
							// Only decrement if nextIndex hasn't changed concurrently
							if rn.nextIndex[peerAddr] == nextIdx && rn.nextIndex[peerAddr] > 1 {
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
		reply.Success = false
		reply.Term = rn.currentTerm
		return nil
	}

	select {
	case rn.resetTimer <- struct{}{}:
	default:
	}

	// if we are a candidate, older leader and see a valid hearbeat, we lost the election so stepdown
	if args.Term > rn.currentTerm {
		rn.stepDown(args.Term)
	} else if rn.state == CANDIDATE && args.Term == rn.currentTerm {
		// another node won the election for this term
		rn.state = FOLLOWER
	}

	rn.currentLeader = args.LeaderID
	reply.Term = rn.currentTerm

	// handle log entries
	lastLogIndex := rn.getLastLogIndex()

	if args.PrevLogIndex < rn.LastIncludedIndex {
		reply.Success = false
		return nil
	}

	if args.PrevLogIndex > lastLogIndex {
		return nil
	}

	// translate array before checking term
	realPrevIndex := rn.getRealIndex(args.PrevLogIndex)

	if rn.log[realPrevIndex].Term != args.PrevLogTerm {
		return nil
	}

	// if we reach here that means the logs match
	// we trucate our lugs to remove any uncommited trash from old leaders
	rn.log = rn.log[:realPrevIndex+1]
	rn.log = append(rn.log, args.Entries...)
	rn.persist()

	// update commit index
	if args.LeaderCommit > rn.commitIndex {
		lastNewEntryIndex := rn.getLastLogIndex()

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

	index := rn.getLastLogIndex() + 1
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
		realN := rn.getRealIndex(n)
		if rn.log[realN].Term != rn.currentTerm {
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
		CurrentTerm:       rn.currentTerm,
		VotedFor:          rn.votedFor,
		Log:               rn.log,
		Snapshot:          rn.snapshot,
		LastIncludedIndex: rn.LastIncludedIndex,
		LastIncludedTerm:  rn.LastIncludedTerm,
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
	rn.snapshot = state.Snapshot
	rn.LastIncludedIndex = state.LastIncludedIndex
	rn.LastIncludedTerm = state.LastIncludedTerm

	// catch up volatile pointers to snapshot
	rn.lastApplied = rn.LastIncludedIndex
	rn.commitIndex = rn.LastIncludedIndex

	fmt.Printf("Node %s restored from disk! Term %d, Log Lenght: %d\n", rn.id, rn.currentTerm, len(rn.log))
}

// converts a global raft log index into the local array index
func (rn *RaftNode) getRealIndex(raftIndex int) int {
	return raftIndex - rn.LastIncludedIndex
}

// offset to retrieve data
func (rn *RaftNode) getLastLogIndex() int {
	return rn.LastIncludedIndex + len(rn.log) - 1
}

// called by the application to compress the Raft log
func (rn *RaftNode) Snapshot(snapshotIndex int, snapshotBytes []byte) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// reject if old or already compressed
	if snapshotIndex < rn.LastIncludedIndex {
		return
	}

	realIndex := rn.getRealIndex(snapshotIndex)

	rn.LastIncludedTerm = rn.log[realIndex].Term
	rn.LastIncludedIndex = snapshotIndex
	rn.snapshot = snapshotBytes

	// CHOP THE LOG WITH AN AXE!
	// create a new slice, index 0 becomes a dummy
	// entry holding the snapshot metadata {Index: 0, Term: 0}
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Index: snapshotIndex, Term: rn.LastIncludedTerm}

	// append what entries came after the snapshot
	newLog = append(newLog, rn.log[realIndex+1:]...)
	rn.log = newLog

	// save to disk
	rn.persist()

	fmt.Printf("Node %s created snapshot at index %d! Log compressed to %d entries.\n", rn.id, snapshotIndex, len(rn.log))

}

func (rn *RaftNode) InstallSnaphot(args InstallSnaphotArgs, reply *InstallSnaphotReply) error {
	rn.mu.Lock()
	reply.Term = rn.currentTerm

	if args.Term < rn.currentTerm {
		rn.mu.Unlock()
		return nil
	}

	if args.Term > rn.currentTerm || rn.state == CANDIDATE {
		rn.stepDown(args.Term)
		reply.Term = rn.currentTerm
	}

	// reset election timer
	select {
	case rn.resetTimer <- struct{}{}:
	default:
	}

	// if we already have a newer or equal snapshot disregard this one
	if args.LastIncludedIndex <= rn.LastIncludedIndex {
		rn.mu.Unlock()
		return nil
	}

	// retain entries that come after the snapshot index if term matches
	var newLog []LogEntry
	newLog = append(newLog, LogEntry{
		Index: args.LastIncludedIndex,
		Term:  args.LastIncludedTerm,
	})

	if args.LastIncludedIndex < rn.getLastLogIndex() {
		realIdx := rn.getRealIndex(args.LastIncludedIndex)
		if realIdx > 0 && realIdx < len(rn.log) && rn.log[realIdx].Term == args.LastIncludedTerm {
			newLog = append(newLog, rn.log[realIdx+1:]...)
		}
	}

	rn.log = newLog
	rn.LastIncludedIndex = args.LastIncludedIndex
	rn.LastIncludedTerm = args.LastIncludedTerm
	rn.snapshot = args.Data

	// advance volatile markers past snapshot
	if args.LastIncludedIndex > rn.LastIncludedIndex {
		rn.currentTerm = args.LastIncludedIndex
	}

	if args.LastIncludedIndex > rn.lastApplied {
		rn.lastApplied = args.LastIncludedIndex
	}

	rn.persist()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rn.mu.Unlock()

	// pass the snapshot to the state machine outside the lock
	go func(msg ApplyMsg) {
		rn.ApplyCh <- msg
	}(applyMsg)

	return nil
}

// return the number of uncompacted log entries currently in memory
func (rn *RaftNode) RaftStateSize() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return len(rn.log)
}
