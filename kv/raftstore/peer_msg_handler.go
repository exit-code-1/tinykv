package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// if return true, d.proposals[0] is the target proposal
func (d *peerMsgHandler) clearStaleAndGetTargetProposal(entry *eraftpb.Entry) bool {
	d.clearStaleProposals(entry)
	if len(d.proposals) > 0 && d.proposals[0].index == entry.Index {
		p := d.proposals[0]
		if p.term != entry.Term {
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			return false
		} else {
			return true
		}
	} else {
		return false
	}
}

func (d *peerMsgHandler) clearStaleProposals(entry *eraftpb.Entry) {
	var i int
	for i = 0; i < len(d.proposals) && d.proposals[i].index < entry.Index; i++ {
		d.proposals[i].cb.Done(ErrResp(&util.ErrStaleCommand{}))
	}
	d.proposals = d.proposals[i:]
}

func (d *peerMsgHandler) processAdminRequest(entry *eraftpb.Entry, cmd *raft_cmdpb.RaftCmdRequest) {
	admin := cmd.AdminRequest
	var p *proposal
	matched := d.clearStaleAndGetTargetProposal(entry)
	if matched {
		p = d.proposals[0]
	}
	wb := &engine_util.WriteBatch{}
	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		cl := admin.CompactLog
		applyState := d.peerStorage.applyState

		// ✅ 更新 TruncatedState
		applyState.TruncatedState.Index = cl.CompactIndex
		applyState.TruncatedState.Term = cl.CompactTerm

		wb.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
		wb.WriteToDB(d.peerStorage.Engines.Kv)
		// ✅ 安排 GC 日志任务
		d.ScheduleCompactLog(cl.CompactIndex)

		// ✅ 响应 proposal
		if matched && p != nil {
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_CompactLog,
				},
			}
			p.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		tl := admin.TransferLeader

		msg := eraftpb.Message{
			MsgType: eraftpb.MessageType_MsgTransferLeader,
			From:    tl.Peer.Id,
			To:      d.PeerId(), // 自己触发
		}

		_ = d.RaftGroup.Step(msg)

		if matched && p != nil {
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
				},
			}
			p.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
	case raft_cmdpb.AdminCmdType_Split:
		sp := admin.Split
		leftRegion := d.Region()
		if cmd.Header.RegionId != leftRegion.Id {
			if matched && p != nil {
				d.proposals = d.proposals[1:]
			}

			return
		}

		if err := util.CheckRegionEpoch(cmd, leftRegion, true); err != nil {
			if matched && p != nil {
				p.cb.Done(ErrResp(err))
				d.proposals = d.proposals[1:]
			}
			return
		}
		if err := util.CheckKeyInRegion(sp.SplitKey, leftRegion); err != nil {
			if matched && p != nil {
				p.cb.Done(ErrResp(err))
				d.proposals = d.proposals[1:]
			}
			return
		}

		if len(leftRegion.Peers) != len(sp.NewPeerIds) {
			if matched && p != nil {
				p.cb.Done(ErrResp(errors.New("len(leftRegion.Peers) != len(sp.NewPeerIds)")))
				d.proposals = d.proposals[1:]
			}
			return
		}

		hasCurrentStore := false
		// 构造新 region（rightRegion）
		rightRegion := &metapb.Region{}
		err := util.CloneMsg(leftRegion, rightRegion)
		if err != nil {
			if matched && p != nil {
				p.cb.Done(ErrResp(err))
				d.proposals = d.proposals[1:]
			}
			return
		}
		newPeers := make([]*metapb.Peer, 0)
		for i, peer := range leftRegion.Peers {
			newPeers = append(newPeers, &metapb.Peer{
				Id:      sp.NewPeerIds[i],
				StoreId: peer.StoreId,
			})
			if peer.StoreId == d.ctx.store.GetId() {
				hasCurrentStore = true
			}
		}
		if len(newPeers) == 0 || !hasCurrentStore {
			if matched && p != nil {
				p.cb.Done(ErrResp(errors.New("len(newPeers) == 0 || !hasCurrentStore")))
				d.proposals = d.proposals[1:]
			}
			return
		}

		// 更新 RegionEpoch
		leftRegion.RegionEpoch.Version++
		rightRegion.RegionEpoch.Version++

		// 设置新 region id, key range, peers
		rightRegion.Id = sp.NewRegionId
		rightRegion.StartKey = sp.SplitKey
		rightRegion.EndKey = leftRegion.EndKey
		rightRegion.Peers = newPeers
		leftRegion.EndKey = sp.SplitKey

		// 持久化 region state 到 rocksdb
		wb := &engine_util.WriteBatch{}
		d.ctx.storeMeta.Lock()
		defer d.ctx.storeMeta.Unlock()
		if d.ctx.storeMeta.regions[sp.NewRegionId] != nil {
			if matched && p != nil {
				p.cb.Done(nil)
				d.proposals = d.proposals[1:]
			}
			return
		}
		// 创建新 peer
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender,
			d.ctx.engine, rightRegion)
		if err != nil {
			panic(err)
		}
		// 更新 regionRanges 和 regions map
		d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: d.Region()})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})
		d.ctx.storeMeta.setRegion(leftRegion, d.peer)
		d.ctx.storeMeta.setRegion(rightRegion, newPeer)
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		meta.WriteRegionState(wb, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(wb, rightRegion, rspb.PeerState_Normal)
		wb.WriteToDB(d.peerStorage.Engines.Kv)

		d.ctx.router.register(newPeer)

		// 启动新 peer
		d.ctx.router.send(rightRegion.GetId(), message.Msg{Type: message.MsgTypeStart})

		// 通知 scheduler（两个 region 都需要）
		d.notifyHeartbeatScheduler(leftRegion, d.peer)
		d.notifyHeartbeatScheduler(rightRegion, newPeer)

		// 返回结果
		if matched && p != nil {
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split: &raft_cmdpb.SplitResponse{
						Regions: []*metapb.Region{leftRegion, rightRegion},
					},
				},
			}
			p.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
	}
}

func (d *peerMsgHandler) handleConfChange(entry *eraftpb.Entry) {
	var cc eraftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		panic(err)
	}

	cmd := &raft_cmdpb.RaftCmdRequest{}
	if err := cmd.Unmarshal(cc.Context); err != nil {
		panic(err)
	}
	admin := cmd.AdminRequest
	cp := admin.ChangePeer

	region := d.Region()

	exists := false
	for _, peer := range region.Peers {
		if peer.Id == cp.Peer.Id {
			exists = true
			break
		}
	}

	if cc.ChangeType == eraftpb.ConfChangeType_AddNode && exists {
		return
	}
	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && !exists {
		return
	}

	matched := d.clearStaleAndGetTargetProposal(entry)
	var p *proposal
	if matched {
		p = d.proposals[0]
	}

	if err := util.CheckRegionEpoch(cmd, region, true); err != nil {
		if matched && p != nil {
			p.cb.Done(ErrResp(err))
			d.proposals = d.proposals[1:]
		}
		return
	}

	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode &&
		cp.Peer.StoreId == d.storeID() {
		if cp.Peer.Id == d.PeerId() && len(d.Region().Peers) <= 2 {
			// 找一个不是自己的 peer 尝试转移 leader
			for _, peer := range d.Region().Peers {
				if peer.Id != d.PeerId() {
					d.RaftGroup.TransferLeader(peer.Id)
					break
				}
			}
			return
		}
		d.startToDestroyPeer()
		return
	}

	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		region.Peers = append(region.Peers, cp.Peer)
	case eraftpb.ConfChangeType_RemoveNode:
		newPeers := make([]*metapb.Peer, 0, len(region.Peers))
		for _, peer := range region.Peers {
			if peer.Id != cp.Peer.Id {
				newPeers = append(newPeers, peer)
			}
		}
		region.Peers = newPeers
	}

	region.RegionEpoch.ConfVer++
	d.peerStorage.SetRegion(region)

	wb := &engine_util.WriteBatch{}
	meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
	wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	wb.WriteToDB(d.peerStorage.Engines.Kv)

	if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regions[d.regionId] = region
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		d.ctx.storeMeta.Unlock()
		d.insertPeerCache(cp.Peer)
	} else if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		d.removePeerCache(cp.Peer.Id)
	}

	d.RaftGroup.ApplyConfChange(cc)

	if matched && p != nil {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			},
		}
		p.cb.Done(resp)
		d.proposals = d.proposals[1:]
	}
	d.notifyHeartbeatScheduler(region, d.peer)
}

func (d *peerMsgHandler) startToDestroyPeer() {
	if len(d.Region().Peers) == 2 && d.IsLeader() {
		var targetPeer uint64 = 0
		for _, peer := range d.Region().Peers {
			if peer.Id != d.PeerId() {
				targetPeer = peer.Id
				break
			}
		}
		if targetPeer == 0 {
			panic("This should not happen")
		}

		m := []eraftpb.Message{{
			To:      targetPeer,
			MsgType: eraftpb.MessageType_MsgHeartbeat,
			Commit:  d.peerStorage.raftState.HardState.Commit,
		}}
		for i := 0; i < 10; i++ {
			d.Send(d.ctx.trans, m)
			time.Sleep(100 * time.Millisecond)
		}
	}
	d.destroyPeer()
}

func (d *peerMsgHandler) process(entry *eraftpb.Entry) {
	// 1. 解码 entry.Data 为 RaftCmdRequest
	var cmd raft_cmdpb.RaftCmdRequest
	if err := cmd.Unmarshal(entry.Data); err != nil {
		panic(err)
	}

	// 2. AdminRequest 走另一条逻辑（假设不涉及 Proposal 直接处理）
	if cmd.AdminRequest != nil {
		d.processAdminRequest(entry, &cmd)
		return
	}

	// 3. 初始化响应结构
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{},
	}

	// 4. 创建写批准备写入 KvDB
	wb := &engine_util.WriteBatch{}

	// 5. Proposal 匹配和清理函数，返回匹配的 proposal（或 nil）
	matchedProposal := d.matchProposal(entry)
	// 6. 执行具体请求，写入 KvDB 并构造响应（只有命中 Proposal 才回复客户端）
	for _, req := range cmd.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			if err := util.CheckRegionEpoch(&cmd, d.Region(), true); err != nil {
				if matchedProposal != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
				}
				return
			}
			if err := util.CheckKeyInRegion(req.Put.Key, d.Region()); err != nil {
				if matchedProposal != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
				}
				return
			}
			wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			d.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value))
			if matchedProposal != nil {
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
			}
		case raft_cmdpb.CmdType_Delete:
			if err := util.CheckRegionEpoch(&cmd, d.Region(), true); err != nil {
				if matchedProposal != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
				}
				return
			}
			if err := util.CheckKeyInRegion(req.Delete.Key, d.Region()); err != nil {
				if matchedProposal != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
				}
				return
			}
			wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
			d.SizeDiffHint -= uint64(len(req.Delete.Key))
			if matchedProposal != nil {
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			}
		case raft_cmdpb.CmdType_Get:
			if matchedProposal != nil {
				if err := util.CheckRegionEpoch(&cmd, d.Region(), true); err != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
					return
				}
				if err := util.CheckKeyInRegion(req.Get.Key, d.Region()); err != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
					return
				}
				val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				if err != nil {
					panic(err)
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: val},
				})
			}
		case raft_cmdpb.CmdType_Snap:
			if matchedProposal != nil {
				if err := util.CheckRegionEpoch(&cmd, d.Region(), true); err != nil {
					matchedProposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
					return
				}
				region := new(metapb.Region)
				err := util.CloneMsg(d.Region(), region)
				if err != nil {
					panic(err)
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: region},
				})
				matchedProposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
		default:
			if matchedProposal != nil {
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Invalid,
				})
			}
		}
	}

	// 7. 更新 applyIndex 并持久化
	d.peerStorage.applyState.AppliedIndex = entry.Index
	wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	wb.WriteToDB(d.peerStorage.Engines.Kv)

	// 8. Proposal 命中时回调 Done 并移除
	if matchedProposal != nil {
		matchedProposal.cb.Done(resp)
		d.proposals = d.proposals[1:]
	}
}

// matchProposal 根据 entry 匹配 proposal，并清理过期 proposal
func (d *peerMsgHandler) matchProposal(entry *eraftpb.Entry) *proposal {
	for len(d.proposals) > 0 {
		p := d.proposals[0]

		if entry.Index < p.index {
			// entry 还没到 proposal 这么前面的index，等后续entry
			return nil
		}

		if entry.Index > p.index {
			// proposal 过期，回调错误并移除
			p.cb.Done(ErrRespStaleCommand(p.term))
			d.proposals = d.proposals[1:]
			continue
		}

		// entry.Index == p.index，判断 term
		if entry.Term == p.term {
			return p
		}

		if entry.Term < p.term {
			// proposal 过期，回调错误并移除
			p.cb.Done(ErrRespStaleCommand(p.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Term > p.term {
			// 乱序，回调错误并移除
			p.cb.Done(ErrRespStaleCommand(p.term))
			d.proposals = d.proposals[1:]
			continue
		}
	}

	return nil
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}

	// 判断是否有新的 Ready 需要处理
	if d.RaftGroup.HasReady() {
		// 获取 Ready 对象，包含待持久化和待处理的数据
		ready := d.RaftGroup.Ready()

		// 将 Ready 中需要持久化的内容（如日志、快照等）保存到底层存储
		result, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			return
		}
		if result != nil && !reflect.DeepEqual(result.PrevRegion, result.Region) {
			d.peerStorage.SetRegion(result.Region)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[result.Region.GetId()] = result.Region
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
			storeMeta.Unlock()
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}

		// 发送 Ready 中产生的 raft 消息给其他 peer
		d.Send(d.ctx.trans, ready.Messages)

		// 处理已提交但尚未应用的日志条目，更新状态机
		for _, entry := range ready.CommittedEntries {
			if len(entry.Data) > 0 {
				if entry.EntryType == eraftpb.EntryType_EntryNormal {
					d.process(&entry)
				} else {
					d.handleConfChange(&entry)
				}
				if d.stopped {
					return
				}
			}
		}

		// 调用 Advance 推进 RawNode，表示已处理当前 Ready，准备下一轮
		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	proposal := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	data, marErr := msg.Marshal()
	if marErr != nil {
		cb.Done(ErrResp(marErr))
		return
	}
	if admin := msg.AdminRequest; admin != nil {
		switch admin.CmdType {
		case raft_cmdpb.AdminCmdType_ChangePeer:
			// 构造 ConfChange
			cc := eraftpb.ConfChange{
				ChangeType: admin.ChangePeer.ChangeType,
				NodeId:     admin.ChangePeer.Peer.Id,
				Context:    data,
			}
			// 调用 ProposeConfChange
			if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.proposals = append(d.proposals, proposal)
			return
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			adminResp := &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header:        &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: adminResp,
			})
		case raft_cmdpb.AdminCmdType_Split:
			if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
				log.Infof("[AdminCmdType_Split] Region %v Split, a expired request", d.Region())
				cb.Done(ErrResp(err))
				return
			}
			if err := util.CheckKeyInRegion(admin.Split.SplitKey, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return
			}
		}
	}
	for _, req := range msg.Requests {
		if req.CmdType != raft_cmdpb.CmdType_Snap {
			if err := util.CheckKeyInRegion(getRequestKey(req), d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return
			}
		}
	}
	perr := d.RaftGroup.Propose(data)
	if perr != nil {
		cb.Done(ErrResp(perr))
		return
	}

	d.proposals = append(d.proposals, proposal)
}

func getRequestKey(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	}
	return nil
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
