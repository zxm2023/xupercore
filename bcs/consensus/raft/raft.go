// Package raft 共识实现包
package raft

import (
	"github.com/xuperchain/xupercore/kernel/consensus"
)

func init() {
	// register consensus "raft"
	_ = consensus.Register("raft", NewRaftConsensus)
}
