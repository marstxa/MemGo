package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/marstxa/handler"
	"github.com/marstxa/raft"
	"github.com/marstxa/resp"
	"github.com/marstxa/store"
	waitregistry "github.com/marstxa/waitRegistry"
)

const SNAPSHOT_THRESHOLD = 50 // Compact whenever uncompacted log exceed x(default: 50) entries

func main() {
	port := flag.Int("port", 6379, "TCP port to listen on for clients")
	id := flag.String("id", "", "This node's Raft address (e.g. localhost:6001)")
	peersFlag := flag.String("peers", "", "Comman-separated list of peer Raft addresses")

	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	// initialise redis and handler
	s := store.New()
	h := handler.New(s)

	// initialise raft consensus engine
	applyCh := make(chan raft.ApplyMsg, 100)
	raftNode := raft.NewRaftNode(*id, peers, applyCh)
	reg := waitregistry.NewWaitRegistry()

	err := raftNode.StartServer()
	if err != nil {
		fmt.Println("Failed to start Raft server:", err)
		return
	}

	go runStateMachine(raftNode, applyCh, h, s, reg)

	// listening
	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	fmt.Println("Listening for Redis clients on", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		go handleConn(conn, h, raftNode, reg)

	}
}

// listens for commited log entries and applies them to the Redis
func runStateMachine(rn *raft.RaftNode, applyCh chan raft.ApplyMsg, h *handler.Handler, s *store.Store, reg *waitregistry.WaitRegeistry) {
	for msg := range applyCh {
		if msg.SnapshotValid {
			err := s.RestoreSnapshot(msg.Snapshot)
			if err != nil {
				fmt.Printf("Failed to apply snapshot: %v\n", err)
				continue
			}
			fmt.Printf("State machine restored at index %d\n", msg.SnapshotIndex)
			continue
		}

		if msg.CommandValid {
			parser := resp.NewResp(bytes.NewReader(msg.Command))
			value, err := parser.Read()
			if err != nil {
				fmt.Println("Error parsing commited command:", err)
				continue
			}

			command := strings.ToUpper(value.Array[0].Bulk)
			args := value.Array[1:]

			if commandFunc, ok := h.Handlers[command]; ok {
				commandFunc(args)
				fmt.Printf("State machine applied %s command successfully.\n", commandFunc)

				// wake up any waiting TCP clients
				reg.Notify(msg.CommandIndex)
			}

			// check if log exceed threshold
			if rn.RaftStateSize() > SNAPSHOT_THRESHOLD {
				snapData, err := s.ExportSnapshot()
				if err != nil {
					fmt.Printf("Failed to export snapshot: %v\n", err)
					continue
				}

				// instruct raft to discard entires up to this applied index
				rn.Snapshot(msg.CommandIndex, snapData)
			}
		}
	}
}

func handleConn(conn net.Conn, h *handler.Handler, rn *raft.RaftNode, reg *waitregistry.WaitRegeistry) {
	defer conn.Close()

	for {
		parser := resp.NewResp(conn)
		value, err := parser.Read()
		if err != nil {
			return
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]
		writer := resp.NewWriter(conn)

		// Intercept CLUSTER commands
		if command == "CLUSTER" && len(args) >= 2 {
			subCommand := strings.ToUpper(args[0].Bulk)
			peerAddr := args[1].Bulk

			var changeType raft.EntryType
			if subCommand == "JOIN" {
				changeType = raft.ADD_NODE_ENTRY
			} else if subCommand == "LEAVE" {
				changeType = raft.REMOVE_NODE_ENTRY
			} else {
				writer.Write(resp.Value{Typ: "error", Str: "ERR unknown CLUSTER subcommand"})
				continue
			}

			// submit to raft
			isLeader, logIndex := rn.SubmitConfigChange(changeType, peerAddr)

			if !isLeader {
				leaderID := rn.GetLeader()
				var errMsg string
				if leaderID == "" {
					errMsg = "ERR Cluster is currently electing a new leader. Please try again."
				} else {
					// FIX: Changed Sprint to Sprintf
					errMsg = fmt.Sprintf("ERR MOVED to Leader %s", leaderID)
				}
				writer.Write(resp.Value{Typ: "error", Str: errMsg})
				continue
			}

			// register a ch for the log index
			waitChan := reg.Register(logIndex)

			// pause the loop and wait for cluster to reach consensus
			select {
			case <-waitChan:
				writer.Write(resp.Value{Typ: "string", Str: "OK"})
			case <-time.After(2 * time.Second):
				writer.Write(resp.Value{Typ: "error", Str: "ERR timeout waiting for consensus"})
			}
			continue // We are done with the CLUSTER command, loop back!
		}

		// Handle normal Redis commands (SET, GET, etc)
		commandFunc, ok := h.Handlers[command]
		if !ok {
			writer.Write(resp.Value{Typ: "error", Str: "ERR unknown command"})
			continue
		}

		isWrite := command == "SET" || command == "HSET" || command == "DEL" || command == "HDEL"

		if isWrite {
			// convert to raw bytes
			var buf bytes.Buffer
			tempWriter := resp.NewWriter(&buf)
			tempWriter.Write(value)

			// submit to raft
			isLeader, logIndex := rn.Submit(buf.Bytes())

			if !isLeader {
				leaderID := rn.GetLeader()
				var errMsg string
				if leaderID == "" {
					errMsg = "ERR Cluster is currently electing a new leader. Please try again."
				} else {
					// FIX: Changed Sprint to Sprintf
					errMsg = fmt.Sprintf("ERR MOVED to Leader %s", leaderID)
				}
				writer.Write(resp.Value{Typ: "error", Str: errMsg})
				continue
			}

			// Wait for consensus
			waitChan := reg.Register(logIndex)
			select {
			case <-waitChan:
				// Only AFTER consensus do we apply the command to the local store and say OK
				writer.Write(resp.Value{Typ: "string", Str: "OK"})
			case <-time.After(2 * time.Second):
				writer.Write(resp.Value{Typ: "error", Str: "ERR timeout waiting for consensus"})
			}
			continue
		}

		// Handle Read-Only commands (GET, PING, etc)
		result := commandFunc(args)
		writer.Write(result)
	}
}
