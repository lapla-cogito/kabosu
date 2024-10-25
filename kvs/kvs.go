package kvs

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
	"time"

	"kabosu/lib"
	"kabosu/raft"

	hraft "github.com/hashicorp/raft"
	"github.com/tidwall/redcon"
)

type Kvs struct {
	listen      net.Listener
	store       lib.Store
	stableStore hraft.StableStore
	id          hraft.ServerID
	raft        *hraft.Raft
}

type KVCmd struct {
	Op  int
	Key []byte
	Val []byte
}

func NewKvs(id hraft.ServerID, raft *hraft.Raft, store lib.Store, stableStore hraft.StableStore) *Kvs {
	return &Kvs{
		store:       store,
		raft:        raft,
		id:          id,
		stableStore: stableStore,
	}
}

func (r *Kvs) Serve(addr string) error {
	var err error
	r.listen, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	return r.handle()
}

var prefixRedisAddr = []byte("___redisAddr")

func FindByNodeID(store hraft.StableStore, lid hraft.ServerID) (string, error) {
	v, err := store.Get(append(prefixRedisAddr, []byte(lid)...))
	if err != nil {
		return "", err
	}

	return string(v), nil
}

func (r *Kvs) handle() error {
	return redcon.Serve(r.listen,
		func(conn redcon.Conn, cmd redcon.Command) {
			err := r.validateCmd(cmd)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			r.processCmd(conn, cmd)
		},

		func(conn redcon.Conn) bool {
			return true
		},

		func(conn redcon.Conn, err error) {
			if err != nil {
				log.Default().Println("error:", err)
			}
		},
	)
}

var argsLen = map[string]int{
	"GET": 2,
	"SET": 3,
	"DEL": 2,
}

const (
	commandName = 0
	keyName     = 1
	value       = 2
)

var ErrArgsLen = errors.New("ERR wrong number of arguments for command")

func (r *Kvs) validateCmd(cmd redcon.Command) error {
	if len(cmd.Args) == 0 {
		return ErrArgsLen
	}

	if len(cmd.Args) < argsLen[string(cmd.Args[commandName])] {
		return ErrArgsLen
	}

	plainCmd := strings.ToUpper(string(cmd.Args[commandName]))
	if len(cmd.Args) != argsLen[plainCmd] {
		return ErrArgsLen
	}

	return nil
}

func (r *Kvs) processCmd(conn redcon.Conn, cmd redcon.Command) {
	ctx := context.Background()

	if r.raft.State() != hraft.Leader {
		_, lid := r.raft.LeaderWithID()
		add, err := FindByNodeID(r.stableStore, lid)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		conn.WriteError("MOVED -1 " + add)
		return
	}

	plainCmd := strings.ToUpper(string(cmd.Args[commandName]))
	switch plainCmd {
	case "GET":
		val, err := r.store.Get(ctx, cmd.Args[keyName])
		if err != nil {
			switch {
			case errors.Is(err, errors.New("key not found")):
				conn.WriteNull()
				return
			default:
				conn.WriteError(err.Error())
				return
			}
		}
		conn.WriteBulk(val)
	case "SET":
		kvCmd := &KVCmd{
			Op:  raft.CommandPut,
			Key: cmd.Args[keyName],
			Val: cmd.Args[value],
		}

		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		f := r.raft.Apply(b, time.Second*1)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}

		conn.WriteString("OK")
	case "DEL":
		kvCmd := &KVCmd{
			Op:  raft.CommandDelete,
			Key: cmd.Args[keyName],
		}

		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}

		f := r.raft.Apply(b, time.Second*1)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}

		res := f.Response()

		err, ok := res.(error)
		if ok {
			conn.WriteError(err.Error())
			return
		}

		conn.WriteInt(1)
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	}
}

func (r *Kvs) Close() error {
	return r.listen.Close()
}

func (r *Kvs) Addr() net.Addr {
	return r.listen.Addr()
}
