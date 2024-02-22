package raft

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"

	"github.com/xuperchain/xupercore/lib/logs"
)

const (
	snapshotCatchUpEntriesN uint64 = 20
	defaultProposalN               = 1000 // proposal channel buffer size

	raftDirName          = "raft"
	walDirName           = "wal"
	snapDirName          = "snap"
	defaultSnapCount     = uint64(10)
	defaultTick          = time.Millisecond * 100
	defaultElectionTick  = 10
	defaultHeartbeatTick = 1
)

type commit struct {
	datas      [][]byte
	applyDoneC chan<- struct{}
}

// raftNode defines a node in the raft cluster.
// custom storage and network module
type raftNode struct {
	// raft basic configuration
	id            uint64        // client ID for raft session
	peers         []uint64      // raft peers id list
	waldir        string        // path to WAL directory
	snapdir       string        // path to snapshot directory
	snapCount     uint64        // the number of log to create a snapshot
	ticker        time.Duration // ticker unit of a clock.default 100ms
	electionTick  int           // election timeout
	heartbeatTick int           // heartbeat timeout

	// raft management objects created by basic configuration
	node        raft.Node           // raft consensus module：node
	raftStorage *raft.MemoryStorage // raft storage module：部分数据内存维护，可以灵活的重写替换和读取
	wal         *wal.WAL            // raft storage module：to manage log
	snapshotter *snap.Snapshotter   // raft storage module：to manage snapshot
	cluster     *cluster            // raft network module：to communicate with other nodes

	// raft state of the node
	confState     raftpb.ConfState // 集群配置状态
	snapshotIndex uint64           // snapshot 的日志的Index
	appliedIndex  uint64           // 已经applied的日志的Index

	getSnapshot func() []byte // 获取快照的函数

	// 数据通道
	proposeC    chan []byte            // raft propose 阶段数据chan
	confChangeC chan raftpb.ConfChange // 集群配置变更chan
	commitC     chan *commit           // raft commit 阶段数据chan
	stopC       chan struct{}          // stop chan

	log     logs.Logger
	raftLog *zap.Logger
	locker  sync.Mutex
	started bool
}

func newRaftNode(id uint64, dataDir string, cfg *Config, cluster *cluster, getSnapshot func() []byte, log logs.Logger) (*raftNode, error) {
	r := &raftNode{}
	r.id = id
	r.peers = cfg.clusterPeersID
	r.waldir = path.Join(dataDir, raftDirName, walDirName)
	r.snapdir = path.Join(dataDir, raftDirName, snapDirName)
	r.snapCount = cfg.SnapCount
	if r.snapCount == 0 {
		r.snapCount = defaultSnapCount
	}
	r.ticker = defaultTick
	r.electionTick = cfg.ElectionTick
	if r.electionTick <= 0 {
		r.electionTick = defaultElectionTick
	}
	r.heartbeatTick = cfg.HeartbeatTick
	if r.heartbeatTick <= 0 {
		r.heartbeatTick = defaultHeartbeatTick
	}

	r.cluster = cluster
	r.getSnapshot = getSnapshot

	// 数据通道
	r.proposeC = make(chan []byte, defaultProposalN)
	r.confChangeC = make(chan raftpb.ConfChange, 1) // not blocked
	r.commitC = make(chan *commit)
	r.stopC = make(chan struct{})

	r.log = log
	r.raftLog, _ = zap.NewProduction()
	r.locker = sync.Mutex{}
	r.started = false

	return r, nil
}

func (r *raftNode) Start() error {
	r.log.Info("start raft node", "id", r.id)

	r.locker.Lock()
	defer r.locker.Unlock()
	if r.started {
		return nil
	}

	err := r.cluster.Start()
	if err != nil {
		return errors.WithMessage(err, "start cluster failed")
	}

	// load snapshot and raft logs from disk
	_, err = os.Stat(r.snapdir)
	if err != nil {
		if err = os.MkdirAll(r.snapdir, 0750); err != nil {
			return errors.WithMessage(err, "cannot create dir for snapshot")
		}
	}
	r.snapshotter = snap.New(r.raftLog, r.snapdir)

	oldwal := wal.Exist(r.waldir)
	r.wal, err = r.replayWAL()
	if err != nil {
		return errors.WithMessage(err, "replay wal failed")
	}
	if r.wal == nil {
		panic("wal is nil")
	}

	// start node
	c := &raft.Config{
		ID:                        r.id,
		ElectionTick:              r.electionTick,
		HeartbeatTick:             r.heartbeatTick,
		Storage:                   r.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	// config already exists in raft storage
	if oldwal {
		r.log.Info("restart raft node")
		r.node = raft.RestartNode(c)
	} else {
		r.log.Info("first startup raft node")
		rpeers := make([]raft.Peer, len(r.peers))
		for i := range rpeers {
			rpeers[i] = raft.Peer{ID: r.peers[i]}
		}

		r.log.Info("is a new node")

		// peers used to start raft node when first time
		r.node = raft.StartNode(c, rpeers)
	}

	go r.loop()
	go r.listen()

	r.started = true
	return nil
}

func (r *raftNode) loop() {
	snapshot, err := r.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	r.confState = snapshot.Metadata.ConfState
	r.snapshotIndex = snapshot.Metadata.Index
	r.appliedIndex = snapshot.Metadata.Index

	defer func() {
		_ = r.wal.Close()
		r.node.Stop()
	}()

	ticker := time.NewTicker(r.ticker)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		defer func() {
			close(r.proposeC)
			close(r.confChangeC)
		}()
		for {
			select {
			case p := <-r.proposeC:
				// blocks until accepted by raft state machine
				err = r.node.Propose(context.TODO(), p)
				if err != nil {
					r.log.Error("failed to propose", "error", err.Error())
					r.errorOccurred(err)
				}

			case cc := <-r.confChangeC:
				err = r.node.ProposeConfChange(context.TODO(), cc)
				if err != nil {
					r.log.Error("failed to propose when conf changed", "error", err.Error())
					r.errorOccurred(err)
				}

			case <-r.stopC:
				return
			}
		}
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			r.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-r.node.Ready():
			err = r.ready(rd)
			if err != nil {
				r.log.Error("failed to handle ready", "error", err.Error())
				r.errorOccurred(err)
			}

		case <-r.stopC:
			return
		}
	}

	// 之所以拆开成两个协程来监听，是担心proposal和confChange时间太长影响ticker频率
}

// listen to raft network messages
func (r *raftNode) listen() {
	l := r.cluster.Accept()
	defer r.cluster.Close()

	for {
		select {
		case msg := <-l:
			if err := r.node.Step(context.Background(), msg); err != nil {
				r.log.Error("failed to handle step", "error", err.Error())
				r.errorOccurred(err)
			}
		case <-r.stopC:
			return
		}
	}
}

func (r *raftNode) ready(rd raft.Ready) error {
	// Must save the snapshot file and WAL snapshot entry before saving any other entries
	// or hardstate to ensure that recovery after a snapshot restore is possible.
	if !raft.IsEmptySnap(rd.Snapshot) {
		err := r.saveSnap(rd.Snapshot)
		if err != nil {
			return err
		}
	}

	if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) != 0 {
		err := r.wal.Save(rd.HardState, rd.Entries)
		if err != nil {
			return err
		}
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		err := r.raftStorage.ApplySnapshot(rd.Snapshot)
		if err != nil {
			return err
		}
		r.publishSnapshot(rd.Snapshot)
	}

	err := r.raftStorage.Append(rd.Entries)
	if err != nil {
		return err
	}

	r.cluster.Send(r.processMessages(rd.Messages))
	applyDoneC, ok := r.publishEntries(r.entriesToApply(rd.CommittedEntries))
	if !ok {
		return errors.New("failed to publish entries")
	}

	r.maybeTriggerSnapshot(applyDoneC)

	r.node.Advance()

	return nil
}

func (r *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > r.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, r.appliedIndex)
	}
	if r.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[r.appliedIndex-firstIdx+1:]
	}
	return nents
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = r.confState
		}
	}
	return ms
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (r *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([][]byte, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := ents[i].Data
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			r.confState = *r.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					r.cluster.Add(string(cc.Context)) // todo
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == r.id {
					r.log.Warn("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				r.cluster.Remove(string(cc.Context)) // todo
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case r.commitC <- &commit{data, applyDoneC}:
		case <-r.stopC:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	r.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (r *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	r.log.Debug(fmt.Sprintf("publishing snapshot at index %d", r.snapshotIndex))
	defer r.log.Debug(fmt.Sprintf("finished publishing snapshot at index %d", r.snapshotIndex))

	if snapshotToSave.Metadata.Index <= r.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, r.appliedIndex)
	}
	// rc.commitC <- nil // trigger kvstore to load snapshot // todo

	r.confState = snapshotToSave.Metadata.ConfState
	r.snapshotIndex = snapshotToSave.Metadata.Index
	r.appliedIndex = snapshotToSave.Metadata.Index
}

func (r *raftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := r.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := r.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return r.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (r *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if r.appliedIndex-r.snapshotIndex <= r.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-r.stopC:
			return
		}
	}

	r.log.Debug(fmt.Sprintf("start snapshot [applied index: %d | last snapshot index: %d]", r.appliedIndex, r.snapshotIndex))

	snapshot, err := r.raftStorage.CreateSnapshot(r.appliedIndex, &r.confState, r.getSnapshot())
	if err != nil {
		panic(err)
	}
	if err = r.saveSnap(snapshot); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if r.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = r.appliedIndex - snapshotCatchUpEntriesN
	}
	if err = r.raftStorage.Compact(compactIndex); err != nil {
		if err != raft.ErrCompacted {
			panic(err)
		}
	} else {
		r.log.Info(fmt.Sprintf("compacted log at index %d", compactIndex))
	}

	r.snapshotIndex = r.appliedIndex
}

func (r *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(r.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(r.raftLog, r.waldir)
		if err != nil {
			log.Fatalf("raftexample: error listing snapshots (%v)", err)
		}
		snapshot, err := r.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatalf("raftexample: error loading snapshot (%v)", err)
		}
		return snapshot
	}
	return nil
}

// openWAL returns a WAL ready for reading.
func (r *raftNode) openWAL(snapshot *raftpb.Snapshot) (*wal.WAL, error) {
	if !wal.Exist(r.waldir) {
		if err := os.Mkdir(r.waldir, 0750); err != nil {
			return nil, errors.WithMessage(err, "cannot create dir for wal")
		}

		w, err := wal.Create(zap.NewExample(), r.waldir, nil)
		if err != nil {
			return nil, errors.WithMessage(err, "create wal error")
		}
		_ = w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	r.log.Info(fmt.Sprintf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index))

	w, err := wal.Open(zap.NewExample(), r.waldir, walsnap)
	if err != nil {
		return nil, errors.WithMessage(err, "error loading wal")
	}

	return w, nil
}

// replayWAL replays WAL entries into the raft instance.
func (r *raftNode) replayWAL() (*wal.WAL, error) {
	r.log.Info(fmt.Sprintf("replaying WAL of member %d", r.id))

	snapshot := r.loadSnapshot()
	w, err := r.openWAL(snapshot)
	if err != nil {
		return nil, err
	}

	_, st, ents, err := w.ReadAll()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to read WAL")
	}

	r.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		err = r.raftStorage.ApplySnapshot(*snapshot)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to apply snapshot")
		}
	}
	err = r.raftStorage.SetHardState(st)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to set hard state")
	}

	// append to storage so raft starts at the right place in log
	err = r.raftStorage.Append(ents)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to append entries")
	}

	return w, nil
}

// IsLeader is it the leader ?
func (r *raftNode) IsLeader() bool {
	if !r.started {
		return false
	}
	return r.node.Status().Lead == r.id
}

// Status returns the status of raft
func (r *raftNode) Status() raft.Status {
	if !r.started {
		return raft.Status{}
	}
	return r.node.Status()
}

// Propose the first stage of the raft:for propose
func (r *raftNode) Propose(data []byte) {
	r.proposeC <- data
}

// ConfChange adds or removes a node from the cluster
func (r *raftNode) ConfChange(cc raftpb.ConfChange) {
	r.confChangeC <- cc
}

// Committed the channel of committed data
func (r *raftNode) Committed() <-chan *commit {
	return r.commitC
}

// errorOccurred a error occurred
func (r *raftNode) errorOccurred(err error) {
	panic(err)
}

// Close closes the raftNode
func (r *raftNode) Close() {
	r.locker.Lock()
	defer r.locker.Unlock()

	r.close()
}

func (r *raftNode) close() {
	if !r.started {
		return
	}

	close(r.stopC)
	close(r.commitC)
	r.started = false
}

// GetTerm returns the current term
func (r *raftNode) GetTerm() uint64 {
	return r.node.Status().Term
}
