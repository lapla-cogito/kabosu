package lib

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"sync"
	"time"
)

type Txn interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
}

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
	Snapshot() (io.ReadWriter, error)
	Restore(buf io.Reader) error
	Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error
	Close() error
}

type memoryStore struct {
	mtx sync.RWMutex
	m   map[uint64][]byte
	ttl map[uint64]int64

	expire *time.Ticker
}

func NewMemoryStore() Store {
	m := &memoryStore{
		mtx: sync.RWMutex{},
		m:   map[uint64][]byte{},

		ttl: nil,
	}

	m.expire = nil

	return m
}

func (m *memoryStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	return m.m[uint64(hash(key))], nil
}

func (m *memoryStore) Put(ctx context.Context, key []byte, value []byte) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.m[uint64(hash(key))] = value

	return nil
}

func (m *memoryStore) Delete(ctx context.Context, key []byte) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	delete(m.m, uint64(hash(key)))

	return nil
}

func (m *memoryStore) Exists(ctx context.Context, key []byte) (bool, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	_, ok := m.m[uint64(hash(key))]

	return ok, nil
}

func (m *memoryStore) Close() error {
	return nil
}

func (m *memoryStore) Restore(buf io.Reader) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.m = make(map[uint64][]byte)
	m.ttl = make(map[uint64]int64)
	err := gob.NewDecoder(buf).Decode(&m.m)
	if err != nil {
		return err
	}

	return nil
}

func (m *memoryStore) Snapshot() (io.ReadWriter, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(m.m)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (m *memoryStore) Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return f(ctx, m)
}

func hash(key []byte) uint64 {
	var h uint64
	for _, b := range key {
		h += uint64(b)
	}
	return h
}
