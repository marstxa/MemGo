package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/marstxa/aof"
	"github.com/marstxa/handler"
	"github.com/marstxa/resp"
)

func main() {
	fmt.Println("Listening on port :6379")

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

		commandFunc, ok := handler.Handlers[command]
		if !ok {
			fmt.Println("Invalid command in AOF: ", command)
			return
		}

		commandFunc(args)
	})

	// create new server
	l, err := net.Listen("tcp", ":6379")
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
		// go routine to handle concurrent clients
		go handleConn(conn, aofFile)
	}

}

func handleConn(conn net.Conn, aofFile *aof.Aof) {

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

		commandFunc, ok := handler.Handlers[command]

		if !ok {
			fmt.Println("Invalid command ", command)
			writer.Write(resp.Value{Typ: "string", Str: ""})
			continue
		}

		// Write mutations to the AOF file before executing
		if command == "SET" || command == "HSET" {
			aofFile.Write(value)
		}

		result := commandFunc(args)
		writer.Write(result)

	}
}
