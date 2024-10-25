package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/joho/godotenv"
)

type Config struct {
	RaftAddr     string
	RedisAddr    string
	ServerID     string
	DataDir      string
	InitialPeers initialPeersList
}

type initialPeersList []Peer

type Peer struct {
	NodeID    string
	RaftAddr  string
	RedisAddr string
}

func (i *initialPeersList) Set(value string) error {
	nodes := strings.Split(value, ",")
	for _, n := range nodes {
		parts := strings.Split(n, "=")
		if len(parts) != 2 {
			return errors.New("invalid peer format. expected nodeID=raftAddress|RedisAddress")
		}

		addresses := strings.Split(parts[1], "|")
		if len(addresses) != 2 {
			return errors.New("invalid peer format. expected nodeID=raftAddress|RedisAddress")
		}

		*i = append(*i, Peer{
			NodeID:    parts[0],
			RaftAddr:  addresses[0],
			RedisAddr: addresses[1],
		})
	}
	return nil
}

func (i *initialPeersList) String() string {
	return fmt.Sprintf("%v", *i)
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file")
	}

	config := Config{
		RaftAddr:     getEnv("RAFT_ADDRESS", "localhost:50051"),
		RedisAddr:    getEnv("REDIS_ADDRESS", "localhost:6379"),
		ServerID:     getEnv("SERVER_ID", ""),
		DataDir:      getEnv("DATA_DIR", ""),
		InitialPeers: initialPeersList{},
	}

	initialPeersStr := getEnv("INITIAL_PEERS", "")
	if err := config.InitialPeers.Set(initialPeersStr); err != nil {
		log.Fatalf("Invalid initial peers: %v", err)
	}

	validateConfig(config)
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func validateConfig(config Config) {
	if config.ServerID == "" {
		log.Fatal("flag --server_id is required")
	}
	if config.RaftAddr == "" {
		log.Fatal("flag --address is required")
	}
	if config.RedisAddr == "" {
		log.Fatal("flag --redis_address is required")
	}
	if config.DataDir == "" {
		log.Fatal("flag --data_dir is required")
	}
}

const snapshotRetainCount = 2

func newRaft(baseDir, id, address string, fsm hraft.FSM, nodes initialPeersList) (*hraft.Raft, hraft.StableStore, error) {
	c := hraft.DefaultConfig()
	c.LocalID = hraft.ServerID(id)

	ldb, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, err
	}

	sdb, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, err
	}

	fss, err := hraft.NewFileSnapshotStore(baseDir, snapshotRetainCount, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, nil, err
	}

	tm, err := hraft.NewTCPTransport(address, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	r, err := hraft.NewRaft(c, fsm, ldb, sdb, fss, tm)
	if err != nil {
		return nil, nil, err
	}

	cfg := hraft.Configuration{Servers: []hraft.Server{{Suffrage: hraft.Voter, ID: hraft.ServerID(id), Address: hraft.ServerAddress(address)}}}

	for _, peer := range nodes {
		sid := hraft.ServerID(peer.NodeID)
		cfg.Servers = append(cfg.Servers, hraft.Server{Suffrage: hraft.Voter, ID: sid, Address: hraft.ServerAddress(peer.RaftAddr)})
	}

	if err := r.BootstrapCluster(cfg).Error(); err != nil {
		return nil, nil, err
	}

	return r, sdb, nil
}
