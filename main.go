package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/marstxa/aof"
	"github.com/marstxa/handler"
	"github.com/marstxa/replication"
	"github.com/marstxa/resp"
	"github.com/marstxa/store"
)

func main() {
	port := flag.Int("port", 6379, "TCP port to listen on")
	replicaof := flag.String("replicaof", "", "Address of the leader (e.g., 'localhost:6379')")
	flag.Parse()

	// initialise store and handler
	s := store.New()
	h := handler.New(s)

	// data persistence with aof
	aofFile, err := aof.NewAof("database.aof")

	if err != nil {
		fmt.Println("Failed to create AOF: ", err)
		return
	}

	defer aofFile.Close()

	aofFile.Read(func(value resp.Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		commandFunc, ok := h.Handlers[command]
		if !ok {
			fmt.Println("Invalid command in AOF: ", command)
			return
		}

		commandFunc(args)
	})

	var leaderReplicator *replication.LeaderReplicator

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	fmt.Println("Listening for clients on", addr)

	// initialise roles based on flags
	if *replicaof != "" {
		fmt.Println("Starting as a FOLLOWER.\nReplicating from:", *replicaof)

		follower := replication.NewFollowerReplicator(s, aofFile, h)
		err := follower.Connect(*replicaof)

		if err != nil {
			fmt.Println("Follower failed to connect immediately, will rely on reconnect loop:", err)
		}

		// listen to leader in the bg
		go follower.ReceiveLoop()
	} else {
		fmt.Println("Starting as a LEADER.")

		leaderReplicator = replication.NewLeaderReplicator(s, aofFile)

		// open dedicated port for followers
		replPort := fmt.Sprintf("0.0.0.0:%d", *port+1000)
		fmt.Println("Listening for followers on", replPort)

		err := leaderReplicator.Listen(replPort)
		if err != nil {
			fmt.Println("Failed to start replication server:", err)
			return
		}
	}

	// create new server
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	// listen for connections

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		// pass the leaderReplicator down
		go handleConn(conn, aofFile, h, leaderReplicator)
	}

}

func handleConn(conn net.Conn, aofFile *aof.Aof, h *handler.Handler, leader *replication.LeaderReplicator) {
	defer conn.Close()

	for {
		// initialise parser by wrapping the connection
		parser := resp.NewResp(conn)

		// attempt to parse a full RESP value
		value, err := parser.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.Array) == 0 {
			fmt.Println("Invaliid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		writer := resp.NewWriter(conn)

		commandFunc, ok := h.Handlers[command]

		if !ok {
			fmt.Println("Invalid command ", command)
			writer.Write(resp.Value{Typ: "error", Str: "ERR unknown command"})
			continue
		}
		isWrite := command == "SET" || command == "HSET" || command == "DEL" || command == "HDEL"

		// Write mutations to the AOF file before executing
		if isWrite {
			// if leader is nil, server is a follower making it READ-ONLY
			if leader == nil {
				writer.Write(resp.Value{Typ: "error", Str: "READONLY You can't write against a read only replica"})
				continue
			}

			aofFile.Write(value)

			// Convert back to raw RESP
			var buf bytes.Buffer
			tempWriter := resp.NewWriter(&buf)
			tempWriter.Write(value)

			leader.Propagate(buf.Bytes())
		}

		// Execute
		result := commandFunc(args)
		writer.Write(result)

	}
}
