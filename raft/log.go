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
	}
}

func (l *RaftLog) EntriesFrom(index uint64) ([]pb.Entry, error) {
	lastIndex := l.LastIndex()
	return l.Entries(index, lastIndex+1)
}

func (l *RaftLog) Entries(i, j uint64) ([]pb.Entry, error) {
	if len(l.entries) == 0 {
		return nil, nil // 没有日志，返回 ErrCompacted
	}
    firstIndex := l.FirstIndex()
    lastIndex := l.LastIndex()

    // 边界检查
    if i < firstIndex {
        return nil, ErrCompacted
    }
    if j > lastIndex + 1 {
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
        return []pb.Entry{}
    }
    firstIndex := l.FirstIndex()
    if l.stabled < firstIndex {
        return append([]pb.Entry{}, l.entries...)
    }
    start := l.stabled - firstIndex + 1
    if start >= uint64(len(l.entries)) {
        return []pb.Entry{}
    }
    return append([]pb.Entry{}, l.entries[start:]...)
}
// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	firstIndex := l.FirstIndex()
	if l.applied < l.committed {
		return l.entries[l.applied+1 -firstIndex : l.committed+1 -firstIndex]
	}
	return nil
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index 
	}
	// 如果没有日志和快照，返回0
	firstindex, _ := l.storage.FirstIndex()
	return firstindex-1
}


// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
        return l.FirstIndex()
    }
    return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if len(l.entries) > 0 && i >= l.FirstIndex() {
		if i > l.LastIndex() {
			return 0, ErrCompacted
		}
		return l.entries[i-l.FirstIndex()].Term, nil
	} else {
		if l.pendingSnapshot != nil {
			if l.pendingSnapshot.Metadata.Index > i {
				return 0, ErrCompacted
			}
			if l.pendingSnapshot.Metadata.Index == i {
				return l.pendingSnapshot.Metadata.Term, nil
			}
			return l.pendingSnapshot.Metadata.Term, nil
		}
		term, err := l.storage.Term(i)
		return term, err
	}
}