// Package handler implements the command routing and in-memory storage engine.
package handler

import (
	"strconv"
	"strings"
	"time"

	"github.com/marstxa/resp"
	"github.com/marstxa/store"
)

// Handler wraps the Store and the command registry
type Handler struct {
	store    *store.Store
	Handlers map[string]func([]resp.Value) resp.Value
}

// New initializes the handler with a store pointer and maps the commands
func New(s *store.Store) *Handler {
	h := &Handler{store: s}
	h.Handlers = map[string]func([]resp.Value) resp.Value{
		"PING":    h.ping,
		"ECHO":    h.echo,
		"SET":     h.set,
		"GET":     h.get,
		"HSET":    h.hset,
		"HGET":    h.hget,
		"HGETALL": h.hgetall,
		"EXISTS":  h.exists,
		"DELETE":  h.del,
		"HEXISTS": h.hexists,
		"HDELETE": h.hdel,
	}
	return h
}

// ping responds with PONG or the provided string argument.
func (h *Handler) ping(args []resp.Value) resp.Value {
	return resp.Value{Typ: "string", Str: "PONG"}
}

func (h *Handler) echo(args []resp.Value) resp.Value {
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
func (h *Handler) set(args []resp.Value) resp.Value {
	if len(args) != 2 && len(args) != 4 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk
	var exp time.Time

	if len(args) == 4 {
		parsedExp, errResp := expiry(args[2], args[3])
		if errResp.Typ == "error" {
			return errResp
		}
		exp = parsedExp
	}

	h.store.Set(key, value, exp)
	return resp.Value{Typ: "string", Str: "OK"}
}

// get retrieves the string value associated with a key.
func (h *Handler) get(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of argument for 'get' command"}
	}

	key := args[0].Bulk
	value, ok := h.store.Get(key)

	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "bulk", Bulk: value}
}

// hset sets the string value of a hash field.
func (h *Handler) hset(args []resp.Value) resp.Value {
	if len(args) != 3 && len(args) != 5 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	hashKey := args[0].Bulk
	field := args[1].Bulk
	value := args[2].Bulk
	var exp time.Time

	if len(args) == 5 {
		parsedExp, errResp := expiry(args[3], args[4])
		if errResp.Typ == "error" {
			return errResp
		}
		exp = parsedExp
	}

	h.store.HSet(hashKey, field, value, exp)
	return resp.Value{Typ: "string", Str: "OK"}
}

// hget returns the value associated with field in the hash stored at key.
func (h *Handler) hget(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hashKey := args[0].Bulk
	field := args[1].Bulk
	value, ok := h.store.HGet(hashKey, field)

	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "bulk", Bulk: value}
}

// hgetall returns all fields and values of the hash stored at key.
func (h *Handler) hgetall(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hashKey := args[0].Bulk
	hashMap := h.store.HGetAll(hashKey)

	if hashMap == nil {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}

	var responseArray []resp.Value
	for field, value := range hashMap {
		responseArray = append(responseArray, resp.Value{Typ: "bulk", Bulk: field})
		responseArray = append(responseArray, resp.Value{Typ: "bulk", Bulk: value})
	}

	return resp.Value{Typ: "array", Array: responseArray}
}

func (h *Handler) exists(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'exists'"}
	}

	if h.store.Exists(args[0].Bulk) {
		return resp.Value{Typ: "integer", Num: 1}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

// delete keys
func (h *Handler) del(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'delete'"}
	}

	successOps := 0
	for _, arg := range args {
		successOps += h.store.Delete(arg.Bulk)
	}

	return resp.Value{Typ: "integer", Num: successOps}
}

func (h *Handler) hexists(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hexists'"}
	}

	if h.store.HExists(args[0].Bulk, args[1].Bulk) {
		return resp.Value{Typ: "integer", Num: 1}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

// delete keys
func (h *Handler) hdel(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hdelete'"}
	}

	hashKey := args[0].Bulk
	successOps := 0

	// skip the hash key
	for i := 1; i < len(args); i++ {
		successOps += h.store.HDelete(hashKey, args[i].Bulk)
	}

	return resp.Value{Typ: "integer", Num: successOps}
}
