package raft

import (
	"context"
	"errors"
	"github.com/hashicorp/raft"
	"github.com/lapla-cogito/kabosu/kvs"
	"io"
)

type Machine struct {
	kvs kvs.Store
}

func NewMachine(store kvs.Store) *Machine {
	return &Machine{store}
}

type Command struct {
	Type  int
	Key   []byte
	Value []byte
}

const (
	CommandPut = iota
	CommandDelete
)

func (c *Command) Encode() []byte {
	buf := make([]byte, 1+len(c.Key)+len(c.Value))
	buf[0] = byte(c.Type)
	copy(buf[1:], c.Key)
	copy(buf[1+len(c.Key):], c.Value)
	return buf
}

func (c *Command) Decode(buf []byte) error {
	if len(buf) < 1 {
		return errors.New("command buffer too short")
	}
	c.Type = int(buf[0])
	buf = buf[1:]

	if len(buf) < 4 {
		return errors.New("command buffer too short")
	}

	keyLen := int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
	buf = buf[4:]

	if len(buf) < keyLen {
		return errors.New("command buffer too short")
	}
	c.Key = buf[:keyLen]
	c.Value = buf[keyLen:]
	return nil
}

func (m *Machine) Apply(log *raft.Log) interface{} {
	conte := context.Background()
	switch log.Type {
	case raft.LogCommand:
		var cmd Command
		if err := cmd.Decode(log.Data); err != nil {
			return err
		}
		switch cmd.Type {
		case CommandPut:
			return m.kvs.Put(conte, cmd.Key, cmd.Value)
		case CommandDelete:
			return m.kvs.Delete(conte, cmd.Key)
		default:
			return errors.New("unknown command")
		}
	default:
		return errors.New("unknown log type")
	}
}

func (m *Machine) Snapshot() (raft.FSMSnapshot, error) {
	rc, err := m.kvs.Snapshot()
	if err != nil {
		return nil, err
	}

	return &Kabosnapshot{rc}, nil
}

func (m *Machine) Restore(rc io.ReadCloser) error {
	return m.kvs.Restore(rc)
}
