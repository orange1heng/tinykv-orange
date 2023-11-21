package runner

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
)

type RaftLogGCTask struct {
	RaftEngine *badger.DB
	RegionID   uint64
	StartIdx   uint64
	EndIdx     uint64
}

type raftLogGcTaskRes uint64

type raftLogGCTaskHandler struct {
	taskResCh chan<- raftLogGcTaskRes
}

func NewRaftLogGCTaskHandler() *raftLogGCTaskHandler {
	return &raftLogGCTaskHandler{}
}

// gcRaftLog does the GC job and returns the count of logs collected.
func (r *raftLogGCTaskHandler) gcRaftLog(raftDb *badger.DB, regionId, startIdx, endIdx uint64) (uint64, error) {
	// Find the raft log idx range needed to be gc.
	firstIdx := startIdx
	if firstIdx == 0 {
		firstIdx = endIdx
		err := raftDb.View(func(txn *badger.Txn) error {
			startKey := meta.RaftLogKey(regionId, 0)
			ite := txn.NewIterator(badger.DefaultIteratorOptions)
			defer ite.Close()
			if ite.Seek(startKey); ite.Valid() {
				var err error
				if firstIdx, err = meta.RaftLogIndex(ite.Item().Key()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}

	if firstIdx >= endIdx {
		log.Infof("no need to gc, [regionId: %d]", regionId)
		return 0, nil
	}

	raftWb := engine_util.WriteBatch{}
	for idx := firstIdx; idx < endIdx; idx += 1 {
		key := meta.RaftLogKey(regionId, idx)
		raftWb.DeleteMeta(key)
	}
	if raftWb.Len() != 0 {
		if err := raftWb.WriteToDB(raftDb); err != nil {
			return 0, err
		}
	}
	return endIdx - firstIdx, nil
}

func (r *raftLogGCTaskHandler) reportCollected(collected uint64) {
	if r.taskResCh == nil {
		return
	}
	r.taskResCh <- raftLogGcTaskRes(collected)
}

func (r *raftLogGCTaskHandler) Handle(t worker.Task) {
	logGcTask, ok := t.(*RaftLogGCTask)
	if !ok {
		log.Errorf("unsupported worker.Task: %+v", t)
		return
	}
	log.Debugf("execute gc log. [regionId: %d, endIndex: %d]", logGcTask.RegionID, logGcTask.EndIdx)
	// 执行实际的垃圾回收操作，清理Raft日志条目。
	collected, err := r.gcRaftLog(logGcTask.RaftEngine, logGcTask.RegionID, logGcTask.StartIdx, logGcTask.EndIdx)
	if err != nil {
		log.Errorf("failed to gc. [regionId: %d, collected: %d, err: %v]", logGcTask.RegionID, collected, err)
	} else {
		log.Debugf("collected log entries. [regionId: %d, entryCount: %d]", logGcTask.RegionID, collected)
	}
	// 报告已GC的日志条目数，上层可以决定是否需要报告，如果需要报告就初始化一个 taskResCh 来接收报告的信息
	r.reportCollected(collected)
}
