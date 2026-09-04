# 🚀 MemGo (Go-Redis Clone)

A lightweight, in-memory Key-Value store built entirely from scratch in Go.

This project is a learning exercise to understand how databases work under the hood. It implements the **Redis Serialization Protocol (RESP)** from scratch with zero external dependencies, making it 100% compatible with the official `redis-cli`.

## 🧠 What I'm Learning

- Network programming and handling concurrent TCP connections in Go.
- Parsing and serializing the RESP protocol.
- Efficient in-memory data storage and memory management.
- Building a CLI-compatible database server.

## 🛠️ Getting Started

### Prerequisites

1. [Go](https://golang.org/) installed on your machine.
2. The official `redis-cli` installed for testing.

### Running the Server

1. Clone this repository.
2. Ensure you have stopped any local Redis instances (to free up port 6379):
    - macOS: `brew services stop redis`
    - Linux: `sudo systemctl stop redis`
3. Start the custom server:
    ```bash
    go run main.go
    ```
