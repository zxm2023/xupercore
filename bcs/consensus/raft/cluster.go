package raft

import (
	"fmt"
	"hash/fnv"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"

	xctx "github.com/xuperchain/xupercore/kernel/common/xcontext"
	"github.com/xuperchain/xupercore/kernel/network"
	"github.com/xuperchain/xupercore/kernel/network/p2p"
	"github.com/xuperchain/xupercore/lib/logs"
	"github.com/xuperchain/xupercore/lib/timer"
	xuperp2p "github.com/xuperchain/xupercore/protos"
)

const (
	// DefaultNetMsgChanSize is the default size of network msg channel
	DefaultNetMsgChanSize = 1000
)

// cluster wraps the network.Network interface to provide a raft cluster
// used for raft node communication
type cluster struct {
	Network        network.Network // p2p实现
	nodeID2Address sync.Map        // map nodeID:address

	// subscribe to messages from other nodes in the cluster
	msgSubscriber p2p.Subscriber
	msgChan       chan *xuperp2p.XuperMessage

	publishChan chan raftpb.Message

	log logs.Logger
}

// newCluster creates a cluster
func newCluster(n network.Network, addresses ...string) *cluster {
	c := &cluster{
		Network: n,

		msgChan: make(chan *xuperp2p.XuperMessage, DefaultNetMsgChanSize),
	}
	c.log, _ = logs.NewLogger("", "raft:cluster")

	// calculate nodeID from address, and store in nodeID2Address
	for _, address := range addresses {
		nodeID := getNodeID(address)
		c.nodeID2Address.Store(nodeID, address)
	}

	c.log.Info(fmt.Sprintf("cluster node addresses: %v", addresses))

	return c
}

// Send sends messages to other nodes in the cluster
func (c *cluster) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			c.log.Error("send message to 0", "msg:", m)
			continue
		}

		// get address from nodeID
		value, ok := c.nodeID2Address.Load(m.To)
		if !ok {
			c.log.Error(fmt.Sprintf("node %d not found in cluster"))
			continue
		}
		address := value.(string)

		data, err := m.Marshal()
		if err != nil {
			c.log.Error("marshal message error", "err:", err)
			continue
		}

		netMsg := p2p.NewMessageWithData(xuperp2p.XuperMessage_RAFT_MESSAGE, data)
		err = c.Network.SendMessage(createNewBCtx(), netMsg, p2p.WithAccounts([]string{address}))
		if err != nil {
			c.log.Error("send message error", "err:", err)
		}
	}
}

// Accept accepts messages from other nodes in the cluster
func (c *cluster) Accept() <-chan raftpb.Message {
	if c.publishChan == nil {
		c.publishChan = make(chan raftpb.Message, DefaultNetMsgChanSize)
	}

	return c.publishChan
}

// Add adds a new node to the cluster
func (c *cluster) Add(address string) {
	c.log.Info(fmt.Sprintf("add node %s to cluster", address))
	c.nodeID2Address.Store(getNodeID(address), address)
}

// Remove removes a node from the cluster
func (c *cluster) Remove(address string) {
	c.log.Info(fmt.Sprintf("remove node %s to cluster", address))

	c.nodeID2Address.Delete(getNodeID(address))
}

// Start starts the cluster
func (c *cluster) Start() error {
	c.msgSubscriber = c.Network.NewSubscriber(xuperp2p.XuperMessage_RAFT_MESSAGE, c.msgChan)
	err := c.Network.Register(c.msgSubscriber)
	if err != nil {
		return err
	}

	// start a goroutine to handle messages from other nodes
	go func() {
		for msg := range c.msgChan {
			// chan closed
			if msg == nil {
				return
			}

			if c.publishChan == nil {
				c.log.Info(fmt.Sprintf("receive message from %s, but the publishChan is nil",
					msg.GetHeader().From))
				continue
			}

			// get the data from msg
			data, err := p2p.Decompress(msg)
			if err != nil {
				c.log.Warn(fmt.Sprintf("receive message from %s, but decompress error",
					msg.GetHeader().From))
				continue
			}
			if data == nil {
				c.log.Warn(fmt.Sprintf("receive message from %s, but the msgInfo is nil",
					msg.GetHeader().From))
				continue
			}

			//c.log.Info("receive raft message", "message", hex.EncodeToString(data))

			// unmarshal the data to a raftpb.Message
			raftMsg := raftpb.Message{}
			err = raftMsg.Unmarshal(data)
			if err != nil {
				c.log.Error("unmarshal raft message error", "err:", err)
				continue
			}

			// push the raftMsg to the publishChan
			c.publishChan <- raftMsg
		}
	}()

	return nil
}

// Close stops the cluster
func (c *cluster) Close() {
	err := c.Network.UnRegister(c.msgSubscriber)
	if err != nil {
		c.log.Error("unregister network error when close cluster", "err:", err)
	}

	close(c.msgChan)
}

func getNodeID(address string) uint64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(address))
	return uint64(h.Sum32())
}

func createNewBCtx() *xctx.BaseCtx {
	log, _ := logs.NewLogger("", "raft")
	return &xctx.BaseCtx{
		XLog:  log,
		Timer: timer.NewXTimer(),
	}
}
