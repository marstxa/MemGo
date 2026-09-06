// Package handler implements the command routing and in-memory storage engine.
package handler

import (
	"sync"

	"github.com/marstxa/resp"
)

// Handlers maps supported Redis commands to their execution functions.
var Handlers = map[string]func([]resp.Value) resp.Value{
	"PING": ping,
	"SET": set,
	"GET": get,
	"HSET": hset,
	"HGET": hget,
	"HGETALL": hgetall,
}

// SETs is the primary key-value store for Strings.
var SETs = map[string]string{}
// SETsMu ensures thread-safe access to the SETs map.
var SETsMu = sync.RWMutex{}

// HSETs is the key-value store for Hashes (maps within maps).
var HSETs = map[string]map[string]string{}
// HSETsMu ensures thread-safe access to the HSETs map.
var HSETsMu = sync.RWMutex{}

// ping responds with PONG or the provided string argument.
func ping(args []resp.Value) resp.Value {
	return resp.Value{Typ: "string", Str: "PONG"}
}

// set stores a string value at the specified key.
func set(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return resp.Value{Typ: "string", Str: "OK"}
}

// get retrieves the string value associated with a key.
func get(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of argument for 'get' command"}
	}

	key := args[0].Bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok{
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "bulk",  Bulk: value}
}

// hset sets the string value of a hash field.
func hset(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}

	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return resp.Value{Typ: "string", Str: "OK"}
}

// hget returns the value associated with field in the hash stored at key.
func hget(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "bulk", Bulk: value}
}

// hgetall returns all fields and values of the hash stored at key.
func hgetall(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].Bulk

	HSETsMu.RLock()
	hashMap, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok || len(hashMap) == 0 {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}

	var responseArray []resp.Value

	for field, value := range hashMap {
		responseArray = append(responseArray, resp.Value{Typ: "bulk", Bulk: field})
		responseArray = append(responseArray, resp.Value{Typ: "bulk", Bulk: value})
	}

	return resp.Value{Typ: "array", Array: responseArray}
}