package raft

import (
	"encoding/json"
)

/*
{
	"period":3000,
	"snap_count":10,
	"election_tick":10,
	"heartbeat_tick":1,
    "cluster_peers":[
        "TeyyPLpp9L7QAcxHangtcHTu7HUZ6iydY",
        "SmJG3rH2ZzYQ9ojxhbRCPwFiE9y6pD1Co"
    ]
}
*/

// Config raft 共识所需要的节点配置
type Config struct {
	// 配置字段
	Period        int64    `json:"period"`
	SnapCount     uint64   `json:"snap_count"`
	ClusterPeers  []string `json:"cluster_peers"`
	ElectionTick  int      `json:"election_tick"`
	HeartbeatTick int      `json:"heartbeat_tick"`

	// 转换字段
	clusterPeersID []uint64
}

func newConfig(cfg string) (*Config, error) {
	var c Config
	err := json.Unmarshal([]byte(cfg), &c)
	if err != nil {
		return nil, err
	}

	c.clusterPeersID = make([]uint64, len(c.ClusterPeers))
	for i, p := range c.ClusterPeers {
		c.clusterPeersID[i] = getNodeID(p)
	}

	return &c, nil
}
