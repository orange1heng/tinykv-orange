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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	hardState, _, _ := storage.InitialState()

	raftLog := &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		dummyIndex:      firstIndex,
	}

	return raftLog

}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	newFirst, err := l.storage.FirstIndex()
	if err != nil {
		panic("This should not happen.")
	}
	if newFirst > l.dummyIndex {
		//l.entries = l.entries[newFirst-l.dummyIndex:]
		entries := l.entries[newFirst-l.dummyIndex:]
		l.entries = make([]pb.Entry, 0)
		l.entries = append(l.entries, entries...)
		l.dummyIndex = newFirst
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return make([]pb.Entry, 0)
	}
	return l.entries[firstIndex-l.dummyIndex:]
}

func (l *RaftLog) getEntries(start uint64, end uint64) []pb.Entry {
	if end == 0 {
		end = l.LastIndex() + 1
	}
	start, end = start-l.dummyIndex, end-l.dummyIndex
	return l.entries[start:end]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	unstableOffset := l.stabled - l.dummyIndex + 1
	// stabled...last
	return l.entries[unstableOffset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	if l.committed < l.applied {
		log.Panicf("nextEnts: l.committed:%v < l.applied:%v", l.committed, l.applied)
		return nil
	}
	// applied...commit
	return l.entries[l.applied-l.dummyIndex+1 : l.committed-l.dummyIndex+1]
}

func (l *RaftLog) firstIndex() uint64 {
	return l.dummyIndex
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex - 1 + uint64(len(l.entries))
}

func (l *RaftLog) LastTerm() uint64 {
	li := l.LastIndex()
	term, err := l.Term(li)
	if err != nil {
		panic(err)
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	if i >= l.dummyIndex {
		// dummyIndex <= i <= l.LastIndex()
		return l.entries[i-l.dummyIndex].Term, nil
	}

	// 是否等于当前正准备安装的快照的最后一条日志
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// 从持久存储中查找，如果有错误返回，那么只能是快照中的日志
	term, err := l.storage.Term(i)
	return term, err

}

func (l *RaftLog) toEntryIndex(i int) uint64 {
	return uint64(i) + l.firstIndex()
}

func (l *RaftLog) stableTo(index uint64) {
	l.stabled = index
}

func (l *RaftLog) commitTo(toCommit uint64) {
	if l.committed < toCommit {
		if l.LastIndex() < toCommit {
			log.Panicf("toCommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", toCommit, l.LastIndex())
		}
		l.committed = toCommit
	}

}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// 用于选举限制
func (l *RaftLog) isUpToDate(index, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

// appendEntry 添加新的日志，并返回最后一条日志的索引
func (l *RaftLog) appendNewEntrys(ents []*pb.Entry) uint64 {
	for i := range ents {
		l.entries = append(l.entries, *ents[i])
	}
	return l.LastIndex()
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return term == t
}

// toSliceIndex return the idx of the given index in the entries
func (l *RaftLog) toSliceIndex(i uint64) int {
	idx := int(i - l.firstIndex())
	if idx < 0 {
		panic("toSliceIndex: index < 0")
	}
	return idx
}

// maybeCommit 检查一个被大多数节点复制的日志是否需要提交
func (l *RaftLog) maybeCommit(toCommit, term uint64) bool {
	commitTerm, _ := l.Term(toCommit)
	if toCommit > l.committed && commitTerm == term {
		// 只有当该日志被大多数节点复制（函数调用保证），并且日志索引大于当前的commitIndex（Condition 1）
		// 并且该日志是当前任期内创建的日志（Condition 2），才可以提交这条日志
		// 【注】为了一致性，Raft 永远不会通过计算副本的方式提交之前任期的日志，只能通过提交当前任期的日志一并提交之前所有的日志
		l.commitTo(toCommit)
		return true
	}
	return false
}
