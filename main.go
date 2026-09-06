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

	// create new server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		return
	}

	// data persistence with aof
	aof, err := aof.NewAof("database.aof")

	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aof.Read(func(value resp.Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		handler, ok := handler.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	// listen for connections
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		return
	}
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

		handler, ok := handler.Handlers[command]
		
		if !ok {
			fmt.Println("Invalid command ", command)
			writer.Write(resp.Value{Typ: "string", Str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)

	}
}