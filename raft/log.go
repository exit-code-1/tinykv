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
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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
	firstIndex uint64
	
	lastIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
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
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:          storage,
		entries:          entries,
		committed:        hardState.Commit,
		applied:          firstIndex - 1,
		stabled:          lastIndex,
		pendingSnapshot:  nil,
		firstIndex: 	firstIndex,
		lastIndex: 		lastIndex,
	}
}

func (l *RaftLog) EntriesFrom(index uint64) ([]pb.Entry, error) {
	lastIndex := l.LastIndex()
	return l.Entries(index, lastIndex+1)
}

func (l *RaftLog) Entries(i, j uint64) ([]pb.Entry, error) {
    firstIndex := l.FirstIndex()
    lastIndex := l.LastIndex()

    // 边界检查
    if i < firstIndex {
        return nil, ErrCompacted
    }
    if j > lastIndex+1 {
        return nil, ErrUnavailable
    }
    if i > j {
        return nil, nil // 空区间合法，返回空 slice
    }

    offsetStart := i - firstIndex
    offsetEnd := j - firstIndex

    return l.entries[offsetStart:offsetEnd], nil
}


func entriesToPointers(entries []pb.Entry) []*pb.Entry {
    res := make([]*pb.Entry, len(entries))
    for i := range entries {
        res[i] = &entries[i]
    }
    return res
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

func (l *RaftLog) maybeCommit(prs map[uint64]*Progress, term uint64) uint64 {
	// 收集所有节点的 matchIndex
	var matchIndexes []uint64
	for _, pr := range prs {
		matchIndexes = append(matchIndexes, pr.Match)
	}

	// 排序后取中位数（多数派能复制到的最大 index）
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] < matchIndexes[j]
	})
	quorumIndex := matchIndexes[(len(matchIndexes)-1)/2]

	// 只有当前 term 的日志才允许被 commit（Raft 论文 §5.4.2）
	t, err := l.Term(quorumIndex)
	if err == nil && t == term {
		return quorumIndex
	}

	return l.committed // 不推进
}

func (l *RaftLog) commitTo(toCommit uint64) {
	if toCommit > l.committed {
		lastIndex := l.LastIndex()
		if toCommit > lastIndex {
			panic("trying to commit beyond last index")
		}
		l.committed = toCommit
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	return append([]pb.Entry{}, l.entries...)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	if l.stabled < l.firstIndex-1 {
		// 全部都没稳定，返回全部
		return l.allEntries()
	}
	// 只返回 index > stabled 的部分
	return append([]pb.Entry{}, l.entries[l.stabled-l.firstIndex+1:]...)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	if l.applied < l.committed {
		return l.entries[l.applied+1-l.firstIndex : l.committed+1-l.firstIndex]
	}
	return nil
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.firstIndex
}

func (l *RaftLog) SetLastIndex(index uint64) {
	// Your Code Here (2A).
	if index < l.firstIndex {
		log.Panicf("SetLastIndex: index %d is less than firstIndex %d", index, l.firstIndex)
	}
	l.lastIndex = index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 1. 如果 snapshot 存在并且 i == snapshot.Metadata.Index，直接返回 snapshot 的 Term
	if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	entries, err := l.Entries(i, i+1)
	if err == nil && len(entries) > 0 {
		return entries[0].Term, nil
	}
	// 如果 entriesFrom 失败，尝试从 storage 获取 term
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return term, nil
}
