package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
    fmt.Println("Listening on port :6379")

    // Create a new server
    l, err := net.Listen("tcp", ":6379")
    if err != nil {
        fmt.Println(err)
        return
    }

    // Listen for connections
    conn, err := l.Accept()
    if err != nil {
        fmt.Println(err)
        return
    }

    defer conn.Close()

    for {
        buffer := make([]byte, 1024)

        // read message from client
        _, err = conn.Read(buffer)
        if err != nil {
            if err == io.EOF {
                break
            }
            fmt.Println("error reading from client: ", err.Error())
            os.Exit(1)
        }

        // ignore
        conn.Write([]byte("+OK\r\n"))
    }
}