package main

import (
	"fmt"
	"net"

	"github.com/marstxa/resp"
)

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Listening on port :6379...")

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
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

		// print the parsed command array to the server logs
		fmt.Printf("Parsed Command: %+v\n", value.Array)

		// ignore request and send back PONG
		conn.Write([]byte("+OK\r\n"))
	}
}