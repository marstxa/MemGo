package waitregistry

import "sync"

// WaitRegeistry maps Raft log indexed to waiting TCP connections

type WaitRegeistry struct {
	mu      sync.Mutex
	waiters map[int]chan struct{}
}

func NewWaitRegistry() *WaitRegeistry {
	return &WaitRegeistry{
		waiters: make(map[int]chan struct{}),
	}
}

// creates a channel for a specific log index and returns it
func (w *WaitRegeistry) Register(index int) chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	ch := make(chan struct{})
	w.waiters[index] = ch
	return ch
}

// wakes up the waiting connection for a specific log index
func (w *WaitRegeistry) Notify(index int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if ch, ok := w.waiters[index]; ok {
		close(ch)
		delete(w.waiters, index)
	}
}
