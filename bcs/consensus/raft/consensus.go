// Package raft 共识接口实现
package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	xctx "github.com/xuperchain/xupercore/kernel/common/xcontext"
	"github.com/xuperchain/xupercore/kernel/consensus"
	common "github.com/xuperchain/xupercore/kernel/consensus/base/common"
	cctx "github.com/xuperchain/xupercore/kernel/consensus/context"
	"github.com/xuperchain/xupercore/kernel/consensus/def"
	"github.com/xuperchain/xupercore/lib/logs"
	"github.com/xuperchain/xupercore/lib/utils"
)

// raftConsensus raft consensus implementation struct.
// Application layer of etcd raft lib
type raftConsensus struct {
	ctx    cctx.ConsensusCtx
	status *Status // consensus running status

	// used for:
	// 1. as snapshot data source
	// 2. append block in order
	blockHeight int64 // current block height
	config      *Config

	node *raftNode // raft 共识节点；记账节点该字段为nil

	// used to block the proposal of a block
	// store:after proposal a block, wait for the block to be committed
	// load:after commit a block
	// delete:after commit a block
	blockID2WaitC sync.Map // blockID -> chan struct{}

	closeC chan struct{}
	log    logs.Logger
}

// NewRaftConsensus 创建 raft共识实例
// cCfg 从创世块中获取的共识配置，是固定的
func NewRaftConsensus(cCtx cctx.ConsensusCtx, cCfg def.ConsensusConfig) consensus.ConsensusImplInterface {
	// basic check
	if cCtx.GetLog() == nil {
		return nil
	}

	if cCtx.Address == nil {
		cCtx.GetLog().Error("consensus::raft::NewRaftConsensus::address is nil")
		return nil
	}

	if cCfg.ConsensusName != ConsensusNameRaft {
		cCtx.GetLog().Error(fmt.Sprintf("consensus name not match,need %s,got %s",
			ConsensusNameRaft, cCfg.ConsensusName))
		return nil
	}

	// parse consensus config
	cfg, err := newConfig(cCfg.Config)
	if err != nil {
		cCtx.GetLog().Error("raft::newConfig error", "error", err)
		return nil
	}

	var (
		peersAddresses = cfg.ClusterPeers     // 集群节点列表
		address        = cCtx.Address.Address // 当前节点地址
		status         = Status{
			Validators: cfg.ClusterPeers,
			PeerIDs:    cfg.clusterPeersID,
		} // 共识状态
		dataDir = cCtx.EnvCfg.GenDirAbsPath(cCtx.EnvCfg.DataDir) // raft数据根目录
	)

	// tip block height
	var blockHeight int64 = -1
	tipBlock := cCtx.Ledger.GetTipBlock()
	if tipBlock != nil {
		blockHeight = tipBlock.GetHeight()
	}

	r := &raftConsensus{
		ctx:         cCtx,
		status:      &status,
		blockHeight: blockHeight,
		config:      cfg,
		closeC:      make(chan struct{}),
		log:         cCtx.GetLog(),
	}

	// check whether current node is a member of cluster
	var isMember bool
	for _, peer := range peersAddresses {
		if address == peer {
			isMember = true
			break
		}
	}

	if !isMember {
		return r
	}

	var getSnapshot = func() []byte {
		return []byte(strconv.FormatInt(r.blockHeight, 10))
	}

	// used to comm
	c := newCluster(cCtx.Network, peersAddresses...)

	r.node, err = newRaftNode(getNodeID(address), dataDir, cfg, c, getSnapshot, cCtx.GetLog())
	if err != nil {
		cCtx.GetLog().Error("raft::newRaftNode error", "error", err)
		return nil
	}

	r.status.node = r.node

	return r
}

// CompeteMaster 判断当前节点是否有出块权利以及是否需要同步区块
// 如果是raft主则有权出块
// 根据raft协议可知，raft 主节点拥有最新状态，所以不需要向其他节点同步区块
func (r *raftConsensus) CompeteMaster(height int64) (bool, bool, error) {
	time.Sleep(time.Duration(r.config.Period) * time.Millisecond)
	return r.node != nil && r.node.IsLeader(), false, nil
}

// CheckMinerMatch 验证block的合法性
// 只校验区块基本签名，不验证是否是主节点出块原因在于：
//
//	1.记账节点无法验证谁是主
//	2.当验证block时主已经切换
//	3.可以将主、term、index等信息写入区块，但wal会归档丢失
func (r *raftConsensus) CheckMinerMatch(ctx xctx.XContext, block cctx.BlockInterface) (bool, error) {
	// 检查区块的区块头是否hash正确
	bid, err := block.MakeBlockId()
	if err != nil {
		return false, err
	}
	if !bytes.Equal(bid, block.GetBlockid()) {
		ctx.GetLog().Warn("Raft::CheckMinerMatch::equal blockid error")
		return false, err
	}

	// 验证签名
	// 验证公钥和地址是否匹配
	k, err := r.ctx.Crypto.GetEcdsaPublicKeyFromJsonStr(block.GetPublicKey())
	if err != nil {
		ctx.GetLog().Warn("Raft::CheckMinerMatch::get ecdsa from block error", "error", err)
		return false, err
	}
	chkResult, _ := r.ctx.Crypto.VerifyAddressUsingPublicKey(string(block.GetProposer()), k)
	if !chkResult {
		ctx.GetLog().Warn("Raft::CheckMinerMatch::address is not match public key")
		return false, err
	}

	// 验证公钥和签名是否匹配
	valid, err := r.ctx.Crypto.VerifyECDSA(k, block.GetSign(), block.GetBlockid())
	if err != nil {
		ctx.GetLog().Warn("Raft::CheckMinerMatch::verifyECDSA error",
			"error", err, "sign", block.GetSign())
	}
	return valid, err
}

// ProcessBeforeMiner 开始挖矿前进行相应的处理
//
//	1.回滚到哪个区块id
//	2.区块中要添加的额外数据
//
// raft 共识不会出现回滚
func (r *raftConsensus) ProcessBeforeMiner(height, timestamp int64) ([]byte, []byte, error) {
	storage := common.ConsensusStorage{
		CurTerm:     int64(r.node.GetTerm()),
		CurBlockNum: height,
	}

	storageBytes, _ := json.Marshal(storage)
	return nil, storageBytes, nil
}

// CalculateBlock 矿工挖矿时共识需要做的工作
// 启动raft共识流程，阻塞直到完成raft共识
func (r *raftConsensus) CalculateBlock(block cctx.BlockInterface) error {
	data := Data{
		BlockHeight: block.GetHeight(),
		BlockId:     block.GetBlockid(),
		BlockBytes:  block.GetBytes(),
	}

	dataBytes, _ := json.Marshal(data)

	r.node.Propose(dataBytes)

	r.log.Info("raft::calculateBlock", "block height", block.GetHeight(),
		"block id", utils.F(block.GetBlockid()))

	waitC := make(chan struct{})
	r.blockID2WaitC.Store(utils.F(block.GetBlockid()), waitC)

	<-waitC

	// the node exit when the raft node error occurred
	return nil
}

// ProcessConfirmBlock 确认块后进行相应的处理
func (r *raftConsensus) ProcessConfirmBlock(block cctx.BlockInterface) error {
	return nil
}

// GetConsensusStatus 获取区块链共识信息
func (r *raftConsensus) GetConsensusStatus() (consensus.ConsensusStatus, error) {
	return r.status, nil
}

// Start 启动raft共识模块
func (r *raftConsensus) Start() error {
	if r.node != nil {
		err := r.node.Start()
		if err != nil {
			panic(err)
		}

		go r.loop()
	}

	return nil
}

// Stop 停止raft共识模块
func (r *raftConsensus) Stop() error {
	if r.node != nil {
		r.node.close()
	}

	close(r.closeC)
	return nil
}

// ParseConsensusStorage 解析共识存储对象
func (r *raftConsensus) ParseConsensusStorage(block cctx.BlockInterface) (interface{}, error) {
	return nil, nil
}

// loop 循环处理已共识的区块
func (r *raftConsensus) loop() {
	committed := r.node.Committed()
	for {
		select {
		case cs := <-committed:
			if cs == nil { // todo 这里为什么会是nil
				continue
			}

			for _, c := range cs.datas {
				if c == nil {
					continue
				}

				// unmarshal data
				var data Data
				err := json.Unmarshal(c, &data)
				if err != nil {
					r.ctx.GetLog().Warn("Raft::loop::unmarshal data from raft error", "error", err)
					continue
				}

				r.log.Info("raft::loop::committed", "block height", data.BlockHeight,
					"block id", utils.F(data.BlockId))

				// release wait
				if waitC, ok := r.blockID2WaitC.Load(utils.F(data.BlockId)); ok {
					close(waitC.(chan struct{}))
					r.blockID2WaitC.Delete(utils.F(data.BlockId))
				} else {
					// follower or restart
					// will drop the block if restart and repack a new with the same height
					r.log.Warn("raft::loop::waitC not found", "block height", data.BlockHeight,
						"block id", utils.F(data.BlockId))

					if data.BlockHeight > r.blockHeight {
						r.blockHeight = data.BlockHeight
					}
					continue
				}

				if r.blockHeight+1 == data.BlockHeight {
					// todo append block
					r.log.Info("raft::loop::append", "block height", data.BlockHeight,
						"block id", utils.F(data.BlockId))

					r.blockHeight++
					continue
				}

				// renew a block
				// 出现这种情况是因为上个主节点在commit前就挂了
				// 导致新的主节点提交了上一个主节点未提交的block和新主节点提交的block
				// 例如：主节点1打包了区块x提交到raft进行共识之后挂掉了
				// 节点2当选为主节点，查看自己账本高度为x-1，然后开始打包区块x
				// 主节点2同时还会提交节点1未提交的区块x
				// 这就导致主节点2会重复提交两个x区块，第一次是上个主节点未提交的，第二次是新的主节点提交的
				if r.blockHeight+1 > data.BlockHeight {
					e := fmt.Sprintf("raft::loop::block height not match,expect %d,got %d",
						r.blockHeight+1, data.BlockHeight)
					r.log.Warn(e)

					// 因为当前版本有区块回滚机制，所以可以大胆接受
					r.blockHeight = data.BlockHeight

					//panic(e)
					continue
				}

				// 不知道啥时候会出现这种情况，先panic
				if r.blockHeight+1 < data.BlockHeight {
					e := fmt.Sprintf("[NOTICE] raft::loop::block height not match,expect %d,got %d",
						r.blockHeight+1, data.BlockHeight)
					panic(e)
				}
			}

			close(cs.applyDoneC)

		case <-r.closeC:
			return
		}

	}
}

// Data defines the data that will be proposed by raft consensus
type Data struct {
	BlockHeight int64
	BlockId     []byte
	BlockBytes  []byte
}
