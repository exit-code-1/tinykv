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
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
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
	Prs map[uint64]*Progress

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

	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err)
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog := newLog(c.Storage)
	lastIndex := raftLog.LastIndex()

	prs := make(map[uint64]*Progress)
	if len(c.peers) > 0 {
		for _, id := range c.peers {
			prs[id] = &Progress{
				Match: 0,
				Next: lastIndex + 1,
			}
		}
	} else {
		for _, id := range cs.Nodes {
			prs[id] = &Progress{}
		}
	}
	r := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             []pb.Message{},
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: 0,
	}
	if c.Applied > 0 {
		r.RaftLog.appliedTo(c.Applied)
	}
	return r
}

func (r *Raft) updateProgress(id uint64, match, next uint64) {
	pr, ok := r.Prs[id]
	if !ok {
		pr = &Progress{}
		r.Prs[id] = pr
	}
	pr.Match = match
	pr.Next = next
}

func (r *Raft) brocAppend() bool {
	// 'bcastAppend' is a broadcast function that sends 'MessageType_MsgAppend' to all peers.
	// It is called by the leader to replicate logs to followers.
	if r.State != StateLeader {
		return false
	}
	for id := range r.Prs {
		if id == r.id {
			continue // 不给自己发
		}
		if !r.sendAppend(id){
			return false
		}
	}
	return true
}
// 'MessageType_MsgAppend' contains log entries to replicate. A leader calls bcastAppend,
// which calls sendAppend, which sends soon-to-be-replicated logs in 'MessageType_MsgAppend'
// type. When 'MessageType_MsgAppend' is passed to candidate's Step method, candidate reverts
// back to follower, because it indicates that there is a valid leader sending
// 'MessageType_MsgAppend' messages. Candidate and follower respond to this message in
// 'MessageType_MsgAppendResponse' type.
func (r *Raft) sendAppend(to uint64) bool {
	pr, ok := r.Prs[to]
	if !ok {
		log.Infof("[sendAppend] Node %d not found in Peers", to)
		return false
	}

	nextIndex := pr.Next
	prevIndex := nextIndex - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		// panic
	}

	entries, _ := r.RaftLog.EntriesFrom(nextIndex)

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: entriesToPointers(entries), // 可能是空的
		Commit:  r.RaftLog.committed,
	}

	r.send(m)
	return true
}



// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat() {
	if r.State != StateLeader {
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		m := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
		}
		r.send(m)
	}
}


// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				Term:   r.Term,
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
				Commit: r.RaftLog.committed,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				Term:   r.Term,
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
				Commit: r.RaftLog.committed,
			})

			r.heartbeatElapsed = 0
		}
	}
}


// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.State = StateFollower
	r.Lead = lead
}


// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.State = StateCandidate
	r.Lead = None
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = 0
	
	// Leader should propose a noop entry
	entry := pb.Entry{
		Term: r.Term,
		EntryType: pb.EntryType_EntryNormal,
	}
	r.appendEntry(&entry)
	
	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		if id == r.id {
			r.updateProgress(id, lastIndex, lastIndex+1)
		} else {
			r.updateProgress(id, 0, lastIndex)
		}
	}
	// 广播 AppendEntries 消息给所有 followers
	if !r.brocAppend() {
		panic("failed to broadcast append entries")
	}
	r.PendingConfIndex = r.RaftLog.LastIndex() // 设置 PendingConfIndex 为当前日志的最后索引
	if len(r.Prs) == 1 {
		r.maybeCommit()
	}
}

func (r *Raft) appendEntry(entries ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for _, ent := range entries {
		ent.Index = lastIndex + 1
		lastIndex++
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}
}

func (r *Raft) append_or_rewriteEntry(prevLogIndex uint64, entries []*pb.Entry) {
    // 先找到从 prevLogIndex + 1 开始的本地日志切片
    fromIndex := prevLogIndex + 1
	log := r.RaftLog
    // 遍历新日志 entries
    for i, entry := range entries {
        localIndex := fromIndex + uint64(i)
        // 如果本地日志已经有这条日志，检查是否冲突
        if localIndex <= log.LastIndex() {
            localTerm, _ := log.Term(localIndex)
            if localTerm != entry.Term {
                // 冲突：删除本地日志从冲突开始的所有条目
                log.entries = log.entries[:localIndex - log.entries[0].Index]
				if log.stabled >= localIndex {
                    log.stabled = localIndex - 1
                }
                // 追加后续新日志条目
				r.appendEntry(entries[i:]...)
				return
            }
        } else {
			if log.stabled >= localIndex {
				log.stabled = localIndex - 1
			}
            // 本地日志没有这条日志，直接追加
			r.appendEntry(entries[i:]...)
            return
        }
    }
    // 如果完全匹配，没有冲突，啥也不做
}

func (r *Raft) send(m pb.Message) {
	// 注意设置 From 字段
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func rejectMsg(m pb.Message, term uint64) pb.Message {
	reject := pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    term,
		MsgType: m.MsgType,
		Reject:  true,
	}
	return reject
}

func (r *Raft) StartElection() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		// 如果只有一个节点，直接成为 Leader
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	// 1. 发送 RequestVote 消息
	for id := range r.Prs {
		if id == r.id {
			continue 
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: lastTerm,
			Index: lastIndex,
		}
		r.send(m)
	}
}

func (r *Raft) HandlePropose(m pb.Message) {
    if r.State != StateLeader {
        return
    }
	if m.Term < r.Term {
		for _, ent := range m.Entries {
			ent.Term = r.Term 
		}
	}
	r.appendEntry(m.Entries...)
	lastIndex := r.RaftLog.LastIndex()
	r.updateProgress(r.id, lastIndex, lastIndex+1) // 更新自己的进度
    if !r.brocAppend() { // 确认方法名拼写
        panic(ErrProposalDropped)
    }
	r.maybeCommit()
}

func isLocalMsg(msgType pb.MessageType) bool {
	return msgType == pb.MessageType_MsgHup ||
		msgType == pb.MessageType_MsgBeat ||
		msgType == pb.MessageType_MsgPropose
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// 1. 如果消息 term < 当前 term，拒绝处理（除非是本地消息）
	if m.Term < r.Term && !isLocalMsg(m.MsgType) {
		m.Term = r.Term
		r.send(pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		MsgType: m.MsgType,
		Reject:  true,
	})
		return nil
	}
	// 3. 分类型处理
	switch m.MsgType {

	// ===== LOCAL MESSAGES（不会经网络发送） =====
	case pb.MessageType_MsgHup:
		r.StartElection()

	case pb.MessageType_MsgBeat:
		r.sendHeartbeat()

	case pb.MessageType_MsgPropose:
		r.HandlePropose(m)

	// ===== 网络消息 =====
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResp(m)

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)

	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

	case pb.MessageType_MsgTransferLeader:
		// r.handleTransferLeader(m)

	case pb.MessageType_MsgTimeoutNow:
		// r.hup()
	}

	return nil
}

// requests votes for election.
func (r *Raft) handleRequestVote(m pb.Message) {
    // 如果请求的任期小于当前任期，拒绝投票
    if m.Term < r.Term {
        r.send(rejectMsg(m, r.Term))
        return
    }

    // 如果请求的任期大于当前任期，更新当前任期并转换为Follower状态
    if m.Term > r.Term {
        r.becomeFollower(m.Term, None)
    }

    // 检查是否已经投过票
    if r.Vote == None || r.Vote == m.From {
        // 检查候选人的日志是否至少与自己一样新
        lastLogIndex := r.RaftLog.LastIndex()
        lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

        upToDate := m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex)

        if upToDate {
            // 授予投票
            r.Vote = m.From
            r.send(pb.Message{
                MsgType: pb.MessageType_MsgRequestVoteResponse,
                To:      m.From,
                From:    r.id,
                Term:    r.Term,
                Reject:  false,
            })
            return
        }
    }

    // 拒绝投票
    r.send(pb.Message{
        MsgType: pb.MessageType_MsgRequestVoteResponse,
        To:      m.From,
        From:    r.id,
        Term:    r.Term,
        Reject:  true,
    })
}

func (r *Raft) handleVoteResp(m pb.Message) {
    // 如果收到的响应任期大于当前任期，降级成Follower，停止选举
    if m.Term > r.Term {
        r.becomeFollower(m.Term, None)
        return
    }

    // 任期小于当前任期，或者当前不是candidate状态，则忽略
    if m.Term < r.Term || r.State != StateCandidate {
        return
    }

    // 更新投票记录，true表示同意，false表示拒绝
    r.votes[m.From] = !m.Reject

    // 重新统计同意票和拒绝票数量
    voteCount := 0
    denialCount := 0
    for _, vote := range r.votes {
        if vote {
            voteCount++
        } else {
            denialCount++
        }
    }

    quorum := len(r.Prs)/2

    // 达到过半同意票，成为Leader
    if voteCount > quorum {
        r.becomeLeader()
        return
    }

    // 达到过半拒绝票，选举失败，降级成Follower
    if denialCount > quorum {
        r.becomeFollower(r.Term, None)
        return
    }
}

// checkLogMatching 检查 index/term 是否与本地日志匹配。
// 返回 ok 表示是否匹配，冲突 term 和 index 可用于快速回退。
func (r *Raft) checkLogMatching(index, term uint64) (bool, uint64, uint64) {
	lastIndex := r.RaftLog.LastIndex()

	// 日志太短，直接失败
	if index > lastIndex {
		return false, lastIndex + 1, 0
	}

	// 获取指定 index 的 term
	localTerm, err := r.RaftLog.Term(index)
	if err != nil {
		// Term 读取失败视为不匹配
		return false, index, 0
	}

	// term 不匹配，返回冲突信息
	if localTerm != term {
		conflictTerm := localTerm
		conflictIndex := r.RaftLog.findFirstIndexOfTerm(conflictTerm)
		return false, conflictIndex, conflictTerm
	}

	// 匹配成功
	return true, 0, 0
}

func (l *RaftLog) findFirstIndexOfTerm(term uint64) uint64 {
	first := l.FirstIndex()
	for i := l.LastIndex(); i >= first; i-- {
		t, err := l.Term(i)
		if err != nil {
			break
		}
		if t == term {
			// 继续向前找
		} else if t < term {
			return i + 1
		}
	}
	return first
}


// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// log.Infof("[Node %d] <- MsgAppend from %d [term: %d, prevIndex: %d, prevTerm: %d, entries: %d, commit: %d]",
	// 	r.id, m.From, m.Term, m.Index, m.LogTerm, len(m.Entries), m.Commit)

	if m.Term < r.Term {
		// log.Infof("[Node %d] Reject MsgAppend from %d: stale term %d < current %d",
		// 	r.id, m.From, m.Term, r.Term)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
			Index:   r.RaftLog.LastIndex(),
		})
		return
	}

	if m.Term > r.Term {
		// log.Infof("[Node %d] BecomeFollower from %d with higher term %d > %d",
		// 	r.id, m.From, m.Term, r.Term)
		r.becomeFollower(m.Term, m.From)
	}

	if r.State == StateLeader {
		// log.Infof("[Node %d] Ignore MsgAppend: I'm leader", r.id)
		return
	}

	if r.State == StateCandidate {
		// log.Infof("[Node %d] Step down to follower due to MsgAppend from %d", r.id, m.From)
		r.becomeFollower(m.Term, m.From)
	}

	r.electionElapsed = 0
	r.Lead = m.From


	ok, conflictIndex, conflictTerm := r.checkLogMatching(m.Index, m.LogTerm)
	if !ok {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
			Index:   conflictIndex,
			LogTerm: conflictTerm,
		})
		return
	}


	// log.Infof("[Node %d] Append %d entries starting from index %d",
	// 	r.id, len(m.Entries), prevIndex+1)
	r.append_or_rewriteEntry(m.Index, m.Entries)

	if m.Commit > r.RaftLog.committed {
		matchIndex := m.Index + uint64(len(m.Entries))
		newCommit := min(m.Commit, matchIndex)
		// log.Infof("[Node %d] Advance commit index from %d to %d",
		// 	r.id, r.RaftLog.committed, newCommit)
		r.RaftLog.committed = newCommit
	}

	// log.Infof("[Node %d] Accept MsgAppend. Send success response with Index %d",
	// 	r.id, r.RaftLog.LastIndex())
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
	})
}



// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
    if m.Term < r.Term {
        // 拒绝，告诉 leader 你的 term 太旧了
        r.send(pb.Message{
            MsgType: pb.MessageType_MsgHeartbeatResponse,
            To:      m.From,
            From:    r.id,
            Term:    r.Term,
            Reject:  true,
        })
        return
    }

    if m.Term > r.Term {
        r.becomeFollower(m.Term, m.From)
    }

    r.electionElapsed = 0
    r.Lead = m.From

    // follower 根据 leader 传来的 Commit 推进自己的 committed（但不能超过自己的 lastIndex）
	if m.Commit > r.RaftLog.committed {
		matchIndex := m.Index + uint64(len(m.Entries))
		r.RaftLog.committed = min(m.Commit, matchIndex)
	}
    r.send(pb.Message{
        MsgType: pb.MessageType_MsgHeartbeatResponse,
        To:      m.From,
        From:    r.id,
        Term:    r.Term,
        Reject:  false,
    })
}


func (r *Raft) handleHeartbeatResp(m pb.Message) {
  if m.Term > r.Term {
    r.becomeFollower(m.Term, None)
    return
  }
  if m.Commit < r.RaftLog.committed || r.Prs[m.From].Match < r.RaftLog.LastIndex() {
    r.sendAppend(m.From)
  }
}

func (r *Raft) handleAppendResponse(m pb.Message) {
    pr, ok := r.Prs[m.From]
    if !ok {
        // 来自非法节点，忽略
        return
    }
	// log.Infof("[AppendResponse] from=%d reject=%v index=%d match=%d next=%d", m.From, m.Reject, m.Index, pr.Match, pr.Next)

    if m.Reject {
        // Append 被拒绝，Leader 需要回退 nextIndex
        // m.Index 是 hintIndex，帮助我们快速找到可接受的日志位置
        // 如果 m.Index = 0，没有 hint，按传统回退
        if m.Index > 0 {
            pr.Next = m.Index
        } else if pr.Next > 1 {
            pr.Next--
        }
        r.sendAppend(m.From)
        return
    }

    // 成功 append：m.Index 是 follower 最新的 Match Index
    // 一定要保证只有在未 reject 时才更新
    pr.Match = m.Index
    pr.Next = pr.Match + 1

    // 更新 progress 后尝试推进 commit
    r.maybeCommit()
    // Leader transfer: 如果我们正试图把 leadership 移交给这个 follower
    // if r.leadTransferee == m.From && pr.Match == r.RaftLog.LastIndex() {
    //     r.sendTimeoutNow(m.From)
    // }
}


func (r *Raft) maybeCommit() bool {
	// 遍历所有 Progress，找到 matchIndex 的中位数
	mci := r.RaftLog.maybeCommit(r.Prs, r.Term)
	// log.Infof("[maybeCommit] trying to commit up to %d, current committed=%d", mci, r.RaftLog.committed)
	if mci > r.RaftLog.committed {
		r.RaftLog.commitTo(mci)
		r.brocAppend()
		return true
	}
	return false
}


// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
