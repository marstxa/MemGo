// Package handler implements the command routing and in-memory storage engine.
package handler

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marstxa/resp"
)

type CacheEntry struct {
	val string
	exp time.Time
}

// Handlers maps supported Redis commands to their execution functions.
var Handlers = map[string]func([]resp.Value) resp.Value{
	"PING":    ping,
	"ECHO":    echo,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"EXISTS":  exists,
	"DELETE":  del,
	"HEXISTS": hexists,
	"HDELETE": hdel,
}

// SETs is the primary key-value store for Strings.
var SETs = map[string]CacheEntry{}

// SETsMu ensures thread-safe access to the SETs map.
var SETsMu = sync.RWMutex{}

// HSETs is the key-value store for Hashes (maps within maps).
var HSETs = map[string]map[string]CacheEntry{}

// HSETsMu ensures thread-safe access to the HSETs map.
var HSETsMu = sync.RWMutex{}

// ping responds with PONG or the provided string argument.
func ping(args []resp.Value) resp.Value {
	return resp.Value{Typ: "string", Str: "PONG"}
}

func echo(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "bulk", Bulk: args[0].Bulk}
}

// Assigns an expiry time to a value based on the optional argument
func expiry(expCmd, expTime resp.Value) (time.Time, resp.Value) {
	expirationCmd := strings.ToUpper(expCmd.Bulk)
	bulTime := expTime.Bulk
	var expirationTime time.Time

	expTimeInt, err := strconv.ParseInt(string(bulTime), 10, 64)
	if err != nil {
		return time.Time{}, resp.Value{Typ: "error", Str: "ERR invalid expire time"}
	}

	if expirationCmd == "EX" {
		expirationTime = time.Now().Add(time.Second * time.Duration(expTimeInt))
	} else if expirationCmd == "PX" {
		expirationTime = time.Now().Add(time.Millisecond * time.Duration(expTimeInt))
	} else {
		return time.Time{}, resp.Value{Typ: "error", Str: "ERR Invalid command for expiry time"}
	}

	return expirationTime, resp.Value{}
}

// set stores a string value at the specified key.
func set(args []resp.Value) resp.Value {
	if len(args) != 2 && len(args) != 4 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	SETsMu.Lock()
	key := args[0].Bulk
	value := args[1].Bulk
	defer SETsMu.Unlock()

	if len(args) == 4 {
		expTime, err := expiry(args[2], args[3])
		if err.Typ == "error" {
			return err
		}

		SETs[key] = CacheEntry{val: value, exp: expTime}
	} else {
		SETs[key] = CacheEntry{val: value}
	}

	return resp.Value{Typ: "string", Str: "OK"}
}

// get retrieves the string value associated with a key.
func get(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of argument for 'get' command"}
	}

	key := args[0].Bulk

	SETsMu.Lock()
	value, ok := SETs[key]
	defer SETsMu.Unlock()

	if !ok {
		return resp.Value{Typ: "null"}
	}

	if time.Now().After(value.exp) && !value.exp.IsZero() {
		delete(SETs, key)
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "bulk", Bulk: value.val}
}

// hset sets the string value of a hash field.
func hset(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	HSETsMu.Lock()
	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk
	defer HSETsMu.Unlock()

	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]CacheEntry{}
	}

	if len(args) == 5 {
		expTime, err := expiry(args[3], args[4])
		if err.Typ == "error" {
			return err
		}

		HSETs[hash][key] = CacheEntry{val: value, exp: expTime}
	} else {
		HSETs[hash][key] = CacheEntry{val: value}
	}

	return resp.Value{Typ: "string", Str: "OK"}
}

// hget returns the value associated with field in the hash stored at key.
func hget(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	HSETsMu.Lock()
	value, ok := HSETs[hash][key]
	defer HSETsMu.Unlock()

	if !ok {
		return resp.Value{Typ: "null"}
	}

	if time.Now().After(value.exp) && !value.exp.IsZero() {
		delete(HSETs[hash], key)
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "bulk", Bulk: value.val}
}

// hgetall returns all fields and values of the hash stored at key.
func hgetall(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].Bulk

	HSETsMu.Lock()
	hashMap, ok := HSETs[hash]
	defer HSETsMu.Unlock()

	if !ok || len(hashMap) == 0 {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}
	var responseArray []resp.Value

	for field, value := range hashMap {
		if time.Now().After(value.exp) && !value.exp.IsZero() {
			delete(HSETs[hash], field)
			continue
		}
		responseArray = append(responseArray, resp.Value{Typ: "bulk", Bulk: field})
		responseArray = append(responseArray, resp.Value{Typ: "bulk", Bulk: value.val})
	}

	return resp.Value{Typ: "array", Array: responseArray}
}

func exists(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of argyments for command 'exists'"}
	}

	SETsMu.Lock()
	defer SETsMu.Unlock()

	key := args[0].Bulk

	if _, ok := SETs[key]; ok {
		return resp.Value{Typ: "integer", Num: 1}
	}

	return resp.Value{Typ: "integer", Num: 0}

}

// delete keys
func del(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of argyments for command 'delete'"}
	}

	SETsMu.Lock()
	defer SETsMu.Unlock()

	successOps := 0

	for i := 0; i < len(args); i++ {
		key := args[i].Bulk

		if _, ok := SETs[key]; ok {
			delete(SETs, key)
			successOps++
		}
	}

	return resp.Value{Typ: "integer", Num: successOps}
}

func hexists(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of argyments for command 'hexists'"}
	}

	HSETsMu.RLock()
	defer HSETsMu.RUnlock()

	hash := args[0].Bulk
	key := args[1].Bulk

	if _, ok := HSETs[hash][key]; ok {
		return resp.Value{Typ: "integer", Num: 1}
	}

	return resp.Value{Typ: "integer", Num: 0}

}

// delete keys
func hdel(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of argyments for command 'hdelete'"}
	}

	HSETsMu.Lock()
	defer HSETsMu.Unlock()

	successOps := 0

	for i := 1; i < len(args); i++ {
		hash := args[0].Bulk
		key := args[i].Bulk

		if _, ok := HSETs[hash][key]; ok {
			delete(HSETs[hash], key)
			successOps++
		}
	}

	return resp.Value{Typ: "integer", Num: successOps}
}
