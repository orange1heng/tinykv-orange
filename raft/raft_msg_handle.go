package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"sort"
)

// handlePropose leader 追加从上层应用接收到的新日志，并广播给 follower
func (r *Raft) handlePropose(m pb.Message) {
	// 追加到当前节点的日志中
	r.appendEntry(m.Entries)

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	if len(r.Prs) == 1 {
		r.RaftLog.commitTo(r.RaftLog.LastIndex())
	} else {
		r.bcastAppend()
	}
}

func (r *Raft) handleStartElection(m pb.Message) {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),  // 选举限制
			Index:   r.RaftLog.LastIndex(), // 选举限制
		})
	}
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	if r.Term > m.Term {
		resp.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, resp)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, None)
	} else {
		r.Prs[m.From].Match = m.Commit
		// 需要同步日志
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Term < r.Term {
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// 选举安全特性：一个任期只能投票一次
	if m.Term == r.Term && r.Vote != m.From && r.Vote != None {
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
		return
	}
	// 选举限制
	if r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		r.Vote = m.From
	} else {
		resp.Reject = true
	}

	r.msgs = append(r.msgs, resp)

	//if m.Term > r.Term {
	//	r.becomeFollower(m.Term, None)
	//	if r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
	//		r.Vote = m.From
	//	} else {
	//		resp.Reject = true
	//	}
	//} else if m.Term == r.Term {
	//	if r.Vote != m.From && r.Vote != None {
	//		resp.Reject = true
	//	} else {
	//		if r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
	//			r.becomeFollower(m.Term, None)
	//			r.Vote = m.From
	//		} else {
	//			resp.Reject = true
	//		}
	//	}
	//} else {
	//	resp.Reject = true
	//}

}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	count := 0
	for _, agree := range r.votes {
		if agree {
			count++
		}
	}
	majority := len(r.Prs)/2 + 1

	if len(r.votes)-count >= majority { // 大多数拒绝票
		r.becomeFollower(r.Term, None)
	}

	if count >= majority { // 大多数同意票
		r.becomeLeader()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	resp := pb.Message{
		// Your Code Here (2A).
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		LogTerm: None,
		Index:   None,
		Reject:  true,
	}
	if m.Term < r.Term {
		// 第一种情况：收到来自过期leader的请求，直接拒绝
		// Index=None, LogTerm=None, Reject=true
		r.msgs = append(r.msgs, resp)
		return
	}
	r.becomeFollower(m.Term, m.From)

	lastLogIndex := r.RaftLog.LastIndex()

	if m.Index > lastLogIndex {
		// 第二种情况：follower丢失了一些条目
		// Index=lastLogIndex + 1, LogTerm=None, Reject=true
		resp.Index = lastLogIndex + 1
		r.msgs = append(r.msgs, resp)
		return
	}
	// 一致性检查
	if m.Index >= r.RaftLog.firstIndex() {
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		// 优化减少AppendEntries RPC失败的次数
		// 优化之后，一个冲突的任期只需要一次AppendEntries，但是如果有多个冲突任期还需要继续多次AppendEntries
		if logTerm != m.LogTerm {
			index := r.RaftLog.toEntryIndex(sort.Search(r.RaftLog.toSliceIndex(m.Index+1),
				func(i int) bool { return r.RaftLog.entries[i].Term == logTerm }))
			resp.Index = index // 期望leader从这个index(冲突任期的第一个index)开始发送日志
			resp.LogTerm = logTerm
			// 第三种情况：一致性检查失败
			// Index=index, LogTerm=logTerm, Reject=true
			r.msgs = append(r.msgs, resp)
			return
		}
	}

	// 开始复制日志
	for i, entry := range m.Entries {
		// 这个已经保存到快照里面去了，注意raft只是保证已经commit的一定持久化，但是不保证持久化的一定commit
		if entry.Index < r.RaftLog.firstIndex() {
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != entry.Term {
				idx := r.RaftLog.toSliceIndex(entry.Index)
				r.RaftLog.entries[idx] = *entry
				// 将冲突之后的日志条目全部删除
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				r.RaftLog.stableTo(min(r.RaftLog.stabled, entry.Index-1))
			}
		} else {
			n := len(m.Entries)
			for j := i; j < n; j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		}
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.commitTo(min(m.Commit, m.Index+uint64(len(m.Entries))))
	}
	resp.Index = r.RaftLog.LastIndex()
	resp.Reject = false
	// Reject=false
	r.msgs = append(r.msgs, resp)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		index := m.Index // follower希望leader从哪个index开始给它发送日志
		if index == None {
			return
		}
		if m.LogTerm != None { // 一致性检查失败：优化减少AppendEntries RPC失败的次数
			//logTerm := m.LogTerm
			//sliceIndex := sort.Search(len(r.RaftLog.entries),
			//	func(i int) bool { return r.RaftLog.entries[i].Term > logTerm })
			//if sliceIndex != len(r.RaftLog.entries) && r.RaftLog.entries[sliceIndex-1].Term == logTerm {
			//	index = r.RaftLog.toEntryIndex(sliceIndex)
			//}
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}

	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
		r.leaderCommit()
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	resp := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, Term: r.Term}

	meta := m.Snapshot.Metadata

	if m.Term < r.Term {
		resp.Reject = true
	} else if r.RaftLog.committed >= meta.Index {
		resp.Reject = true
		resp.Index = r.RaftLog.committed
	} else {
		// install snapshot.
		r.becomeFollower(m.Term, m.From)

		r.RaftLog.dummyIndex = meta.Index + 1
		r.RaftLog.committed = meta.Index
		r.RaftLog.applied = meta.Index
		r.RaftLog.stabled = meta.Index
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.RaftLog.entries = make([]pb.Entry, 0)

		r.Prs = make(map[uint64]*Progress)
		for _, id := range meta.ConfState.Nodes {
			r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		}

		resp.Index = meta.Index
	}
	r.msgs = append(r.msgs, resp)
}
