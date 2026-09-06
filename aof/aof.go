package aof

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"

	"github.com/marstxa/resp"
)

type Aof struct {
	File *os.File
	Rd   *bufio.Reader
	Mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		return nil, err
	}

	aof := &Aof{
		File: file,
		Rd:   bufio.NewReader(file),
	}

	// start goroutine to sync AOF to disk every 1 second
	go func() {
		for {
			aof.Mu.Lock()
			aof.File.Sync()
			aof.Mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	aof.Mu.Lock()
	defer aof.Mu.Unlock()

	return aof.File.Close()
}

func (aof *Aof) Write(value resp.Value) error {
	aof.Mu.Lock()
	defer aof.Mu.Unlock()

	_, err := aof.File.Write(value.Marshal())
	if err != nil {
		return err
	}

	return nil
}

func (aof *Aof) Read(callback func(value resp.Value)) error {
	aof.Mu.Lock()
	defer aof.Mu.Unlock()

	parser := resp.NewResp(aof.File)

	for {
		value, err := parser.Read()

		if err == nil {
			callback(value)
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}

	return nil
}
