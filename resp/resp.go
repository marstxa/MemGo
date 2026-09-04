package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	Typ string
	Str string
	Num int
	Bulk string
	Array []Value
}

type Resp struct {
	reader *bufio.Reader

}

// NewResp  creates a new wrapper aroundd the TCP connection
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (line []byte, n int, err error){
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}

		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line) -2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err 
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

func (r *Resp) readArray() (Value, error){
	v := Value{}
	v.Typ = "array"

	// read len of the array
	length, _, err := r.readInteger()
	if err != nil {
		return v, err 
	}

	// for each line parse and read the value
	v.Array = make([]Value, length)
	for i:=0;i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err 
		}

		// add parsed value to array
		v.Array[i] = val
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error){
	v := Value{}
	v.Typ = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err 
	}

	bulk := make([]byte, len)
	r.reader.Read(bulk)
	v.Bulk = string(bulk)

	// Read the trailing \r\n
	r.readLine()

	return v, nil
}
 