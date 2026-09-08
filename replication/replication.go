package replication

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/marstxa/aof"
	"github.com/marstxa/handler"
	"github.com/marstxa/resp"
	"github.com/marstxa/store"
)

// tracks connected followers and pushes writes to them
type LeaderReplicator struct {
	mu        sync.Mutex
	followers map[string]net.Conn // follower ID -> its connection
	store     *store.Store        // needed to build a catch-up snapshot for new followers
	aofFile   *aof.Aof            // replay history of lagging/new follower
}

// follower, connects to the leader and applies whatever it streams
type FollowerReplicator struct {
	conn       net.Conn // persisten conn to the leader
	leaderAddr string
	store      *store.Store
	aofFile    *aof.Aof         // writes are persisted locally
	handler    *handler.Handler // follower has access to the commands
}

// == LEADER METHODS ==

func NewLeaderReplicator(store *store.Store, aofFile *aof.Aof) *LeaderReplicator {
	return &LeaderReplicator{
		followers: make(map[string]net.Conn),
		store:     store,
		aofFile:   aofFile,
	}
}

// opens a listener dedicated to follower connections
func (l *LeaderReplicator) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return err
	}

	go l.acceptLoop(ln)

	return nil
}

// accepts incoming follower connections in a loop, hands each to onFollowerConnect
func (l *LeaderReplicator) acceptLoop(ln net.Listener) {
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Replication accept error:", err)
			continue
		}

		// hand off the new follower connection to its own goroutine
		go l.onFollowerConnect(conn)
	}

}

// register the follower, triggers sendCatchUp then keeps it for streaming
func (l *LeaderReplicator) onFollowerConnect(conn net.Conn) {
	id := conn.RemoteAddr().String()

	l.mu.Lock()
	l.followers[id] = conn
	l.mu.Unlock()

	fmt.Printf("New follower connected: %s\n", id)

	// make follower catch up
	err := l.sendCatchUp(conn)
	if err != nil {
		fmt.Printf("Error sending catch-up to %s: %v\n", id, err)
		l.removeFollower(id)
		return
	}
}

// replays AOF so a new Follower can catch up
func (l *LeaderReplicator) sendCatchUp(conn net.Conn) error {
	data, err := os.ReadFile("database.aof")
	if err != nil {
		if os.IsNotExist(err) {
			println("No AOF file exists yet (fresh server)")
			return nil
		}

		return fmt.Errorf("error reading AOF file: %w", err)
	}

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("error streaming catch-up data to follower: %w", err)
	}

	return nil
}

// called by handler after local write
// forwards the same raw RESP bytes to every connected follower, in the order applied
func (l *LeaderReplicator) Propagate(rawCmd []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for id, conn := range l.followers {
		_, err := conn.Write(rawCmd)
		if err != nil {
			fmt.Printf("Failed to write to follower %s: %v\n", id, err)
			conn.Close()
			delete(l.followers, id)
		}
	}

	return nil
}

// clean up when follower connection drops
func (l *LeaderReplicator) removeFollower(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if conn, ok := l.followers[id]; ok {
		conn.Close()
		delete(l.followers, id)
		fmt.Printf("Removed follower: %s\n", id)
	}
}

// == FOLLOWER METHODDS ==

func NewFollowerReplicator(store *store.Store, aofFile *aof.Aof, h *handler.Handler) *FollowerReplicator {
	return &FollowerReplicator{
		store:   store,
		aofFile: aofFile,
		handler: h,
	}
}

// dials leader replication port, left open for process lifetime
func (f *FollowerReplicator) Connect(leaderAddr string) error {
	f.leaderAddr = leaderAddr

	d, err := net.Dial("tcp", leaderAddr)

	if err != nil {
		return fmt.Errorf("Error dialing to leader replication port %v", err)
	}

	f.conn = d

	return nil
}

// reads incoming RESP commands of the connection on at a time
func (f *FollowerReplicator) ReceiveLoop() error {
	// wrap connection
	parser := resp.NewResp(f.conn)

	for {
		// attempt to parse a full RESP value
		value, err := parser.Read()
		if err != nil {
			fmt.Println(err)
			f.reconnect()
			break
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

		f.apply(command, args)
	}

	return nil
}

// routes the received command through the SAME handler function your client-facing
func (f *FollowerReplicator) apply(cmd string, args []resp.Value) error {
	commandFunc, ok := f.handler.Handlers[cmd]

	if !ok {
		return fmt.Errorf("unknown command from leader: %s", cmd)
	}

	if cmd == "SET" || cmd == "HSET" || cmd == "DEL" || cmd == "HDEL" {
		fullCommand := resp.Value{
			Typ:   "array",
			Array: append([]resp.Value{{Typ: "bulk", Bulk: cmd}}, args...),
		}
		f.aofFile.Write(fullCommand)
	}

	commandFunc(args)

	return nil
}

// retry with backoff if the leader connection drops
func (f *FollowerReplicator) reconnect() {
	fmt.Println("Connection to leader lost. Attempting to reconnect...")

	delay := 1 * time.Second
	maxDelay := 30 * time.Second

	for {
		err := f.Connect(f.leaderAddr)
		if err == nil {
			fmt.Println("Successfully reconnected to leader!")

			go f.ReceiveLoop()
			return
		}

		fmt.Printf("Reconnect failed. Retrying in %v...\n", delay)
		time.Sleep(delay)

		// double the delay for the next attempt (Exponential Backoff)
		delay *= 2

		// cap so we dont wait forever
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}
