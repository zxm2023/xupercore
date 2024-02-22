package raft

import (
	"encoding/json"
	"strconv"
)

const ConsensusNameRaft = "raft"

// Status records node consensus status
type Status struct {
	ID         uint64    `json:"id"`         // 节点ID
	Validators []string  `json:"validators"` // 所有共识节点地址
	PeerIDs    []uint64  `json:"peer_ids"`   // 所有共识节点ID,与上述地址一一对应
	Miner      string    `json:"miner"`      // 节点地址，与dpos命名保持一致
	Term       int64     `json:"term"`       // 当前term
	State      string    `json:"state"`      // 状态
	node       *raftNode // raft 共识节点；记账节点该字段为nil
}

// GetVersion returns the consensus version
// meaningless under this consensus
func (s *Status) GetVersion() int64 {
	return 0
}

// GetConsensusBeginInfo returns the initial height of this consensus
// meaningless under this consensus
func (s *Status) GetConsensusBeginInfo() int64 {
	return 0
}

// GetStepConsensusIndex gets the index in the consensus
// slice where the consensus item is located.
// meaningless under this consensus
func (s *Status) GetStepConsensusIndex() int {
	return 0
}

// GetConsensusName the name of the consensus
func (s *Status) GetConsensusName() string {
	return ConsensusNameRaft
}

// GetCurrentTerm the current term the node is in
func (s *Status) GetCurrentTerm() int64 {
	if s.node != nil {
		s.Term = int64(s.node.Status().Term)
	}
	return s.Term
}

// GetCurrentValidatorsInfo returns the current validators info
func (s *Status) GetCurrentValidatorsInfo() []byte {
	if s.node != nil {
		raftStatus := s.node.Status()
		s.ID = s.node.id
		s.Term = int64(raftStatus.Term)
		s.Miner = strconv.FormatInt(int64(raftStatus.Lead), 10)
		s.State = raftStatus.RaftState.String()
	}
	b, _ := json.Marshal(s)
	return b
}
