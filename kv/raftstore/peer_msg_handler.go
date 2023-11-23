package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
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

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	ready := d.RaftGroup.Ready()

	// 1.保存需要持久化的日志、HardState(RaftLocalState)，应用Snapshot
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Panic(err)
	}

	// 应用了快照数据之后，修改元数据
	if applySnapResult != nil && !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
		d.peerStorage.SetRegion(applySnapResult.Region)
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
		storeMeta.regionRanges.Delete(&regionItem{applySnapResult.PrevRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{applySnapResult.Region})
		storeMeta.Unlock()
	}

	// 2.发送msg给其他peer
	d.Send(d.ctx.trans, ready.Messages)

	// 3. apply已经commit的日志
	if len(ready.CommittedEntries) > 0 {
		kvWB := &engine_util.WriteBatch{}
		// 3.1 真正的执行raft日志命令
		for _, ent := range ready.CommittedEntries {
			kvWB = d.processCommittedEntry(&ent, kvWB)
			if d.stopped {
				return
			}
		}
		// 3.2 更新并持久化RaftApplyState
		lastEntry := ready.CommittedEntries[len(ready.CommittedEntries)-1]
		d.peerStorage.applyState.AppliedIndex = lastEntry.Index
		if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Panic(err)
		}

		// 持久化
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	}

	// 4.推进raft
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) processCommittedEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	if entry.EntryType == pb.EntryType_EntryConfChange {
		cc := &pb.ConfChange{}
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Panic(err)
		}
		log.Infof("EntryType_EntryConfChange")
		return d.processConfChange(entry, cc, kvWB)
	}

	request := &raft_cmdpb.RaftCmdRequest{}
	if err := request.Unmarshal(entry.Data); err != nil {
		log.Panic(err)
	}

	if request.AdminRequest != nil {
		return d.processAdminRequest(entry, request, kvWB)
	} else {
		return d.processRequest(entry, request, kvWB)
	}
}

func (d *peerMsgHandler) processConfChange(entry *pb.Entry, cc *pb.ConfChange, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(cc.Context); err != nil {
		log.Panic(err)
	}
	region := d.Region()
	changePeerReq := msg.AdminRequest.ChangePeer

	// 检查 Command Request 中的 RegionEpoch 是否是过期的，以此判定是不是一个重复的请求
	// 实验指导书中提到，测试程序可能会多次提交同一个 ConfChange 直到 ConfChange 被应用
	// CheckRegionEpoch 检查 RaftCmdRequest 头部携带的 RegionEpoch 是不是和 currentRegionEpoch 匹配
	if err, ok := util.CheckRegionEpoch(msg, region, true).(*util.ErrEpochNotMatch); ok {
		log.Infof("[processConfChange] %v RegionEpoch not match", d.PeerId())
		d.handleProposal(entry, ErrResp(err))
		return kvWB
	}

	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode: // 添加一个节点
		log.Infof("[AddNode] %v add %v", d.PeerId(), cc.NodeId)
		if d.searchPeerWithId(cc.NodeId) == len(region.Peers) { // 要添加的peer不存在于当前Region的peers中
			region.Peers = append(region.Peers, changePeerReq.Peer)
			// 更新RegionLocalState状态
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal) // PeerState 用来表示当前节点的 Peer 是否在 region 中
			// 更新 metaStore 中的 region 信息
			d.updateStoreMeta(region)
			// 真正的apply日志：更新 peerCache
			d.insertPeerCache(changePeerReq.Peer)
		}
	case pb.ConfChangeType_RemoveNode: // 删除一个节点
		log.Infof("[RemoveNode] %v remove %v", d.PeerId(), cc.NodeId)
		if cc.NodeId == d.PeerId() {
			d.destroyPeer()
			log.Infof("[RemoveNode] destory %v compeleted", cc.NodeId)
			return kvWB
		}
		n := d.searchPeerWithId(cc.NodeId)
		if n != len(region.Peers) { // 要删除的peer存在于当前region的peers中
			// 删除节点 RaftGroup 中的第 n 个 peer
			region.Peers = append(region.Peers[:n], region.Peers[n+1:]...)
			// 更新RegionLocalState状态
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal) // PeerState 用来表示当前 Peer 是否在 region 中
			// 更新 metaStore 中的 region 信息
			d.updateStoreMeta(region)
			// 真正的apply日志：更新 peerCache
			d.removePeerCache(cc.NodeId)
		}
	}
	// 更新 raft 层的配置信息
	d.RaftGroup.ApplyConfChange(*cc)

	// 处理 proposal
	d.handleProposal(entry, &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region},
		},
	})

	// 新增加的 peer 是通过 leader 的心跳完成的
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return kvWB
}

// searchPeerWithId 根据需要添加或者删除的 Peer id，找到 region 中是否已经存在这个 Peer
func (d *peerMsgHandler) searchPeerWithId(nodeId uint64) int {
	for id, peer := range d.peerStorage.region.Peers {
		if peer.Id == nodeId {
			return id
		}
	}
	return len(d.peerStorage.region.Peers)
}

func (d *peerMsgHandler) updateStoreMeta(region *metapb.Region) {
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.regions[region.Id] = region
	storeMeta.Unlock()
}

// processAdminRequest 处理 commit 的 Admin Request 类型 command
func (d *peerMsgHandler) processAdminRequest(entry *pb.Entry, requests *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	adminReq := requests.AdminRequest
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		if adminReq.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
			truncatedState := d.peerStorage.applyState.TruncatedState
			truncatedState.Index, truncatedState.Term = adminReq.CompactLog.CompactIndex, adminReq.CompactLog.CompactTerm
			if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
				log.Panic(err)
			}
			d.ScheduleCompactLog(adminReq.CompactLog.CompactIndex)
		}
	case raft_cmdpb.AdminCmdType_Split:
		if requests.Header.RegionId != d.regionId {
			regionNotFound := &util.ErrRegionNotFound{RegionId: requests.Header.RegionId}
			d.handleProposal(entry, ErrResp(regionNotFound))
			return kvWB
		}
		// 1.预检查：过期分裂请求检查
		if errEpochNotMatch, ok := util.CheckRegionEpoch(requests, d.Region(), true).(*util.ErrEpochNotMatch); ok {
			d.handleProposal(entry, ErrResp(errEpochNotMatch))
			return kvWB
		}
		if err := util.CheckKeyInRegion(adminReq.Split.SplitKey, d.Region()); err != nil {
			d.handleProposal(entry, ErrResp(err))
			return kvWB
		}
		if len(d.Region().Peers) != len(adminReq.Split.NewPeerIds) {
			log.Warningf("%s len(d.Region().Peers) != len(splitReq.NewPeerIds), region: %v", d.Tag, d.Region())
			d.handleProposal(entry, ErrRespStaleCommand(d.Term()))
			return kvWB
		}

		// 2.分裂准备
		// 修改Region元信息 -> 创建新的region
		oldRegion, split := d.Region(), adminReq.Split
		oldRegion.RegionEpoch.Version++
		newRegion := d.createNewSplitRegion(split, oldRegion) // 创建新的 Region
		// 修改 storeMeta 元信息
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regionRanges.Delete(&regionItem{region: oldRegion})          // 删除 oldRegion 的数据范围
		oldRegion.EndKey = split.SplitKey                                      // 修改 oldRegion 的 range
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion}) // 更新 oldRegion 的 range
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion}) // 创建 newRegion 的 range
		storeMeta.regions[newRegion.Id] = newRegion                            // 设置 regions 映射
		storeMeta.Unlock()
		// 持久化 oldRegion 和 newRegion，regionLocalState
		meta.WriteRegionState(kvWB, oldRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		// 分裂准备完成之后重置统计数据
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		// 3. 创建当前 store 上的 newRegion Peer，注册到 router，并启动计时器
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.schedulerTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			log.Panic(err)
		}
		d.ctx.router.register(newPeer)
		if err := d.ctx.router.send(newRegion.Id, message.Msg{
			Type: message.MsgTypeStart,
		}); err != nil {
			panic("This should not happen")
		}

		// 处理回调函数
		d.handleProposal(entry, &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{newRegion, oldRegion}},
			},
		})

		log.Infof("[AdminCmdType_Split Process] oldRegin %v, newRegion %v", oldRegion, newRegion)
		// 发送 heartbeat 给其他节点，帮助新region快速创建peer
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			d.notifyHeartbeatScheduler(oldRegion, d.peer)
			d.notifyHeartbeatScheduler(newRegion, newPeer)
		}

	default:
		log.Panic("invalid admin request: %v", adminReq.CmdType)
	}
	return kvWB
}

// notifyHeartbeatScheduler 帮助 region 快速创建 peer
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

func (d *peerMsgHandler) createNewSplitRegion(split *raft_cmdpb.SplitRequest, oldRegion *metapb.Region) *metapb.Region {
	// 跟原来的region处于同样的store上
	newPeers := make([]*metapb.Peer, 0)
	for i, peer := range oldRegion.Peers {
		newPeers = append(newPeers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: peer.StoreId})
	}
	newRegion := &metapb.Region{
		Id:          split.NewRegionId,
		StartKey:    split.SplitKey,
		EndKey:      oldRegion.EndKey,
		Peers:       newPeers, // Region 中每个 Peer 的 id 以及所在的 storeId
		RegionEpoch: &metapb.RegionEpoch{Version: InitEpochVer, ConfVer: InitEpochConfVer},
	}
	return newRegion
}

// processRequest 处理 commit 的 Put/Get/Delete/Snap 类型 command
func (d *peerMsgHandler) processRequest(entry *pb.Entry, request *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: make([]*raft_cmdpb.Response, 0),
	}
	// 1.首先检查 Key 是否在 Region 中
	for _, req := range request.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key := req.Get.Key
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				// ErrKeyNotInRegion
				BindRespError(resp, err)
			} else {
				// Get 和 Snap 请求需要先将之前的结果写到 DB.
				// why? 因为这样才能保证能够看到之前的结果
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
				kvWB = &engine_util.WriteBatch{}
				value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: value},
				})
			}
		case raft_cmdpb.CmdType_Put:
			key := req.Put.Key
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				// ErrKeyNotInRegion
				BindRespError(resp, err)
			} else {
				kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
			}
		case raft_cmdpb.CmdType_Delete:
			key := req.Delete.Key
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				// ErrKeyNotInRegion
				BindRespError(resp, err)
			} else {
				kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			}
		case raft_cmdpb.CmdType_Snap:
			//log.Infof("apply snap command, region: %v, req.region:%v", d.Region(), request.Header.RegionId)
			// 如果在获取这个region的scan结果时，发现该region被分裂了，那么应该让上层重试snap请求
			if request.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				// ErrEpochNotMatch
				BindRespError(resp, &util.ErrEpochNotMatch{})
			} else {
				// Get 和 Snap 请求需要先将结果写到 DB，否则的话如果有多个 entry 同时被 apply，客户端无法及时看到写入的结果
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
				kvWB = &engine_util.WriteBatch{}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
				})
			}
		}
	}

	// 处理回调
	d.handleProposal(entry, resp)
	return kvWB
}

// 在apply日志之后，响应命令
func (d *peerMsgHandler) handleProposal(entry *pb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		// 1. 过期回调
		// 如果 leader 宕机了并且有一个新的 leader 向它发送了快照，当应用了快照之后又继续同步了新的日志并 commit 了
		// 这种情况通常发生在日志被截断的情况下，比如在 leader 更换后。
		// 这个时候 proposal.index < entry.index
		if proposal.term < entry.Term || proposal.index < entry.Index {
			// 日志被截断/过期，返回一个过期响应. regionNotFound
			NotifyStaleReq(proposal.term, proposal.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		// 2. 正确匹配：找到该条entry对应的cb
		if proposal.term == entry.Term && proposal.index == entry.Index {
			if proposal.cb != nil {
				// 创建一个新的事务
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false) // snap resp should set txn explicitly
			}
			proposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
		// 3. further proposal：（即当前的 entry 并没有 proposal 在等待，或许是因为现在是 follower 在处理 committed entry）
		return
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

// 将 client 的请求包装成 entry 传递给 raft 层
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			log.Panic(err)
		}
		if err := d.RaftGroup.Propose(data); err != nil {
			log.Panic(err)
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		// 领导者转移不需要等待转移成功才返回结果，它更多的是一个用于"建议"的命令，而不是强制性的
		adminResp := &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header:        &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: adminResp,
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// 单步成员变更：必须前一个成员变更请求apply之后才能处理下一个成员变更
		if d.peerStorage.AppliedIndex() >= d.RaftGroup.Raft.PendingConfIndex {
			// 如果 region 只有两个节点，并且需要 remove leader，则需要先完成 transferLeader
			if len(d.Region().Peers) == 2 && msg.AdminRequest.ChangePeer.ChangeType == pb.ConfChangeType_RemoveNode && msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() {
				for _, p := range d.Region().Peers {
					if p.Id != d.PeerId() {
						d.RaftGroup.TransferLeader(p.Id)
						break
					}
				}
			}
			// propose到raft模块
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb})
			context, _ := msg.Marshal()
			d.RaftGroup.ProposeConfChange(pb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    context,
			})
		}
	case raft_cmdpb.AdminCmdType_Split:
		// 如果收到的 Region Split 请求是一条过期的请求，则不应该提交到 Raft. 检查ConfVer和Version是否匹配（是否中途发生了配置变更/分裂）
		if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
			log.Infof("[AdminCmdType_Split] Region %v Split, a expired request", d.Region())
			cb.Done(ErrResp(err))
			return
		}
		// 检查splitkey是否还在当前region中
		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		log.Infof("[AdminCmdType_Split Propose] Region %v Split, entryIndex %v", d.Region(), d.nextProposalIndex())
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		data, _ := msg.Marshal()
		d.RaftGroup.Propose(data)
	}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	key := getRequestKey(msg.Requests[0])
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	//1. 封装回调，等待log被apply的时候调用
	//  后续相应的 entry 在apply之后，响应该 proposal，即 callback.Done( )；
	d.proposals = append(d.proposals, &proposal{
		index: d.RaftGroup.Raft.RaftLog.LastIndex() + 1,
		term:  d.RaftGroup.Raft.Term,
		cb:    cb,
	})

	//2. 序列化RaftCmdRequest
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	//3. 将该字节流包装成 entry 传递给下层的 raft
	err = d.RaftGroup.Propose(data)
	if err != nil {
		log.Panic(err)
	}
}

func getRequestKey(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	return key
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
