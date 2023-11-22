// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"os"
	"sort"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress // nextIndex[] + matchIndex[]

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee  uint64
	transferElapsed int

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 任何时候，日志中只能有一个待处理（pending）的配置变更。
	// 在一个配置变更日志 applied 之前，不能有其他的配置变更被加入到日志中。
	PendingConfIndex uint64

	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	IsEnableLog := os.Getenv("kv_debug")
	if IsEnableLog != "false" {
		//log.SetLevel(log.LOG_LEVEL_DEBUG)
		log.SetLevel(log.LOG_LEVEL_ERROR)
	} else {
		log.SetLevel(log.LOG_LEVEL_WARN)
	}

	raftLog := newLog(c.Storage)
	hardSt, config, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	if c.peers == nil {
		c.peers = config.Nodes
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		State:            StateFollower,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: 0,
	}

	if !IsEmptyHardState(hardSt) {
		r.loadState(hardSt)
	}

	if c.Applied > 0 {
		raftLog.appliedTo(c.Applied)
	}

	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{
				Match: raftLog.LastIndex(),
				Next:  raftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[peer] = &Progress{
				Match: 0,
				Next:  raftLog.LastIndex() + 1,
			}
		}
	}

	r.resetRandomElectionTimeout()

	r.PendingConfIndex = r.initPendingConfIndex()
	return r
}

// initPendingConfIndex 初始化 pendingConfIndex
// 查找 [appliedIndex + 1, lastIndex] 之间是否存在还没有 Apply 的 ConfChange Entry
func (r *Raft) initPendingConfIndex() uint64 {
	for i := r.RaftLog.applied + 1; i <= r.RaftLog.LastIndex(); i++ {
		if r.RaftLog.entries[i-r.RaftLog.dummyIndex].EntryType == pb.EntryType_EntryConfChange {
			return i
		}
	}
	return None
}

func (r *Raft) resetRandomElectionTimeout() {
	ra := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.randomElectionTimeout = r.electionTimeout + ra.Intn(r.electionTimeout)
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.commitTo(state.Commit)
	r.Term = state.Term
	r.Vote = state.Vote
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	m := pb.Message{
		// Your Code Here (2A).
		MsgType:  pb.MessageType_MsgAppend,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    r.Prs[to].Next - 1,
		Entries:  make([]*pb.Entry, 0),
		Commit:   r.RaftLog.committed,
		Snapshot: nil,
	}
	prevTerm, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	if err != nil {
		r.sendSnapshot(to)
		log.Infof("[Snapshot Request]%d to %d, prevLogIndex %v, dummyIndex %v", r.id, to, r.Prs[to].Next-1, r.RaftLog.dummyIndex)
		return false
	}
	ents := r.RaftLog.getEntries(r.Prs[to].Next, 0)
	m.LogTerm = prevTerm
	for i := range ents {
		m.Entries = append(m.Entries, &ents[i])
	}
	r.msgs = append(r.msgs, m)
	return true
}

// leader发送快照给别的节点
func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		// 生成 Snapshot 的工作是由 region worker 异步执行的，如果 Snapshot 还没有准备好
		// 此时会返回 ErrSnapshotTemporarilyUnavailable 错误，此时 leader 应该放弃本次 Snapshot Request
		// 等待下一次再请求 storage 获取 snapshot（通常来说会在下一次 heartbeat response 的时候发送 snapshot）
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		if err != nil {
			panic(err)
		}
	}
}

func (r *Raft) tickHeartbeat() {
	r.electionElapsed++
	r.heartbeatElapsed++

	if r.State != StateLeader {
		return
	}

	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
	}

	if r.heartbeatTimeout >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		// MessageType_MsgBeat 属于内部消息，不需要经过 RawNode 处理
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		if err != nil {
			panic(err)
		}
	}
	if r.leadTransferee != None {
		// 在一个选举周期后如果领导者转移还未完成，就中止流程，以便恢复客户端请求
		r.transferElapsed++
		if r.transferElapsed >= r.electionTimeout {
			r.leadTransferee = None
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		// 只有 Term > currentTerm 的时候才需要对 Vote 进行重置
		// 这样可以保证在一个任期内只会进行一次投票
		r.Vote = None
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.resetRandomElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.resetRandomElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
	}

	r.PendingConfIndex = r.initPendingConfIndex()

	// 按理来说是广播心跳信息，但是如果广播心跳信息，如果follower日志落后，还是会发送Append消息让其同步日志，这样会浪费一轮心跳信息
	// 所以这里广播Append信息，相当于带上了心跳信息
	// 解决「当前任期内迟迟没有新日志到来，导致之前任期的日志一直无法被“间接”提交」问题
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// todo: 快照、领导人转移、配置变更
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// local msg: MsgHup(领导者选举)
		// 心跳机制：MsgHeartbeat
		// 日志复制：MsgAppend
		// 领导人选举：MsgRequestVote
		// 领导人转移：MsgTransferLeader(负责转发消息给leader), MsgHup(变为领导者)
		r.stepFollower(m)
	case StateCandidate:
		// local msg: MsgHup(领导者选举)
		// 心跳机制：MsgHeartbeat
		// 日志复制：MsgAppend
		// 领导人选举：MsgRequestVote, MsgRequestVoteResponse
		// 领导人转移：MsgTransferLeader(负责转发消息给leader), MsgHup(变为领导者)
		r.stepCandidate(m)
	case StateLeader:
		// MsgPropose
		// 心跳机制：MsgHeartbeat, MsgHeartbeatResponse
		// 日志复制：MsgAppend, MsgAppendResponse
		// 领导人选举：MsgRequestVote
		// 领导人转移：MsgTransferLeader
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNowRequest(m)
	}
}
func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNowRequest(m)
	}

}
func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgBeat:
		r.bcastHeatbeat()
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		r.PendingConfIndex = None // 清除 PendingConfIndex 表示当前没有未完成的配置更新
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		// 如果是删除节点，由于有节点被移除了，这个时候可能有新的日志可以提交
		// 这是必要的，因为 TinyKV 只有在 handleAppendRequestResponse 的时候才会判断是否有新的日志可以提交
		// 如果节点被移除了，则可能会因为缺少这个节点的回复，导致可以提交的日志无法在当前任期被提交
		if r.State == StateLeader && r.maybeCommit() {
			log.Infof("[removeNode commit] %v leader commit new entry, commitIndex %v", r.id, r.RaftLog.committed)
			r.bcastAppend()
		}
	}
}

func (r *Raft) maybeCommit() bool {
	matchArray := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArray = append(matchArray, progress.Match)
	}
	// // 获取所有节点 match 的中位数，就是被大多数节点复制的日志索引
	sort.Sort(sort.Reverse(matchArray))
	majority := len(r.Prs)/2 + 1
	toCommitIndex := matchArray[majority-1]
	// 检查是否可以推进commitIndex
	return r.RaftLog.maybeCommit(toCommitIndex, r.Term)
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) appendEntry(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()

	for i := range entries {
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
		if entries[i].EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entries[i].Index
		}
	}
	r.RaftLog.appendNewEntrys(entries)
}

func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	// 超过半数的人的match >= n，说明超过半数的人已经复制的index都到了n
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		// 保证只能提交当前任期内的日志（Section5.4.2）
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.bcastAppend()
		}
	}
}

func (r *Raft) bcastHeatbeat() {
	for peerId := range r.Prs {
		if r.id != peerId {
			r.sendHeartbeat(peerId)
		}
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, From: r.id, To: to})
}
