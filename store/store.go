package store

import (
	"sync"
	"time"
)

type CacheEntry struct {
	val string
	exp time.Time
}

type Store struct {
	mu     sync.RWMutex // Protects both data and hashes
	data   map[string]CacheEntry
	hashes map[string]map[string]CacheEntry
}

// New initializes the maps to prevent nil pointer panics
func New() *Store {
	return &Store{
		data:   make(map[string]CacheEntry),
		hashes: make(map[string]map[string]CacheEntry),
	}
}

func (s *Store) Set(key, value string, exp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = CacheEntry{val: value, exp: exp}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock() // Using full Lock because we might delete
	defer s.mu.Unlock()

	entry, ok := s.data[key]
	if !ok {
		return "", false
	}

	if time.Now().After(entry.exp) && !entry.exp.IsZero() {
		delete(s.data, key)
		return "", false
	}

	return entry.val, true
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[key]
	return ok
}

func (s *Store) Delete(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; ok {
		delete(s.data, key)
		return 1
	}
	return 0
}

func (s *Store) HSet(key, field, value string, exp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.hashes[key]; !ok {
		s.hashes[key] = make(map[string]CacheEntry)
	}
	s.hashes[key][field] = CacheEntry{val: value, exp: exp}
}

func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.hashes[key][field]
	if !ok {
		return "", false
	}

	if time.Now().After(entry.exp) && !entry.exp.IsZero() {
		delete(s.hashes[key], field)
		return "", false
	}

	return entry.val, true
}

func (s *Store) HGetAll(key string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	hashMap, ok := s.hashes[key]
	if !ok || len(hashMap) == 0 {
		return nil
	}

	result := make(map[string]string)
	for field, entry := range hashMap {
		if time.Now().After(entry.exp) && !entry.exp.IsZero() {
			delete(s.hashes[key], field)
			continue
		}
		result[field] = entry.val
	}
	return result
}

func (s *Store) HExists(key, field string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.hashes[key][field]
	return ok
}

func (s *Store) HDelete(key, field string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.hashes[key][field]; ok {
		delete(s.hashes[key], field)
		return 1
	}
	return 0
}
