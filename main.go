package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/marstxa/handler"
	"github.com/marstxa/raft"
	"github.com/marstxa/resp"
	"github.com/marstxa/store"
)

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
	applyCh := make(chan []byte, 100)
	raftNode := raft.NewRaftNode(*id, peers, applyCh)

	err := raftNode.StartServer()
	if err != nil {
		fmt.Println("Failed to start Raft server:", err)
		return
	}

	go runStateMachine(applyCh, h)

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

		go handleConn(conn, h, raftNode)

	}
}

// listens for commited log entries and applies them to the Redis
func runStateMachine(applyCh chan []byte, h *handler.Handler) {
	for cmdBytes := range applyCh {
		// wrap the raw bytes in a reader so our RESP parser can read it
		parser := resp.NewResp(bytes.NewReader(cmdBytes))

		value, err := parser.Read()
		if err != nil {
			fmt.Println("Error parsing commited command:", err)
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		if commandFunc, ok := h.Handlers[command]; ok {
			// apply to store
			commandFunc(args)
			fmt.Printf("State machine applied %s command succesfully.\n", command)
		}
	}
}

func handleConn(conn net.Conn, h *handler.Handler, rn *raft.RaftNode) {
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
			isLeader, _ := rn.Submit(buf.Bytes())

			if !isLeader {
				// reject the write
				writer.Write(resp.Value{Typ: "error", Str: "ERR I am not the Leader Node"})
				continue
			}

			// For now reply OK immediately
			writer.Write(resp.Value{Typ: "string", Str: "OK"})
			continue
		}

		result := commandFunc(args)
		writer.Write(result)
	}
}
