package raft

import (
	"io"

	"github.com/hashicorp/raft"
)

var _ raft.FSMSnapshot = (*Kabosnapshot)(nil)

type Kabosnapshot struct {
	io.ReadWriter
}

func (f *Kabosnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	_, err := io.Copy(sink, f)
	return err
}

func (f *Kabosnapshot) Release() {
}
