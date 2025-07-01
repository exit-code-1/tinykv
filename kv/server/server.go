package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	start_ts := req.GetVersion()
	txn := mvcc.NewMvccTxn(reader, start_ts)
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return nil, err
	}
	if lock == nil || lock.Ts > start_ts {
		value, err := txn.GetValue(req.GetKey())
		if err != nil {
			return nil, err
		}
		if value == nil {
			return &kvrpcpb.GetResponse{
				NotFound: true,
			}, nil
		}
		return &kvrpcpb.GetResponse{
			Value: value,
		}, nil
	} else {
		lock_info := &kvrpcpb.LockInfo{
			Key:         req.GetKey(),
			PrimaryLock: lock.Primary,
			LockVersion: lock.Ts,
		}
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: lock_info,
			},
		}, nil
	}
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	start_ts := req.GetStartVersion()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, start_ts)
	errors := []*kvrpcpb.KeyError{}
	for _, mutation := range req.Mutations {
		write, late_ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && late_ts > start_ts {
			key_error := &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    start_ts,
					ConflictTs: late_ts,
					Key:        mutation.Key,
				},
			}
			errors = append(errors, key_error)
			continue
		}
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			key_error := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					Key:         mutation.Key,
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
				},
			}
			errors = append(errors, key_error)
			continue
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      start_ts,
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		})
		if mvcc.WriteKindFromProto(mutation.Op) == mvcc.WriteKindPut {
			txn.PutValue(mutation.Key, mutation.Value)
		} else if mvcc.WriteKindFromProto(mutation.Op) == mvcc.WriteKindDelete {
			txn.DeleteValue(mutation.Key)
		}
	}

	// Write the transaction's modifications to storage
	if len(errors) == 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
	}

	return &kvrpcpb.PrewriteResponse{
		Errors: errors,
	}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	commit_ts := req.GetCommitVersion()
	start_ts := req.GetStartVersion()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, commit_ts)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		// Check if there's already a write record for this transaction
		// We need to create a transaction with start_ts to properly search for existing writes
		startTxn := mvcc.NewMvccTxn(reader, start_ts)
		write, _, err := startTxn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		if write != nil {
			// Transaction already committed or rolled back
			if write.Kind == mvcc.WriteKindRollback {
				// Transaction was rolled back
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "transaction was rolled back",
					},
				}, nil
			}
			// Transaction already committed, this is a repeat commit - just ignore
			continue
		}

		if lock == nil {
			// No lock and no write record - this means the transaction was never prewritten
			// According to Percolator protocol, this is treated as a successful no-op
			continue
		}

		if lock.Ts != start_ts {
			// Lock belongs to a different transaction
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "lock conflict",
				},
			}, nil
		}

		// Lock belongs to this transaction, commit it
		txn.PutWrite(key, commit_ts, &mvcc.Write{
			StartTS: start_ts,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	// Write the transaction's modifications to storage
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 创建 MVCC 事务
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	// 创建扫描器
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scanner.Close()

	// 收集结果
	var pairs []*kvrpcpb.KvPair
	limit := req.GetLimit()

	for uint32(len(pairs)) < limit {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}

		// 扫描结束
		if key == nil && value == nil {
			break
		}

		// 检查是否有锁阻塞这个键
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		// 如果有锁并且锁的时间戳大于我们的读取时间戳，跳过这个键
		// 注意：在扫描中，单个键的锁错误不应该停止整个扫描
		if lock != nil && lock.Ts > req.GetVersion() {
			continue
		}

		// 添加到结果中
		pair := &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		pairs = append(pairs, pair)
	}

	return &kvrpcpb.ScanResponse{
		Pairs: pairs,
	}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 使用 LockTs 创建事务来检查事务状态
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())

	// 1. 首先检查是否已经有 Write 记录
	write, commitTs, err := txn.CurrentWrite(req.GetPrimaryKey())
	if err != nil {
		return nil, err
	}

	// 如果有 Write 记录
	if write != nil {
		if write.Kind == mvcc.WriteKindRollback {
			// 已经被回滚了
			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl: 0,
				Action:  kvrpcpb.Action_NoAction,
			}, nil
		} else {
			// 已经被 commit 了，返回 commitTs
			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl:       0,
				CommitVersion: commitTs,
				Action:        kvrpcpb.Action_NoAction,
			}, nil
		}
	}

	// 2. 检查是否有 Lock
	lock, err := txn.GetLock(req.GetPrimaryKey())
	if err != nil {
		return nil, err
	}

	if lock == nil {
		// 没有 lock 也没有 write，说明从未被预写过，或者已经被回滚但没有留下记录
		// 创建一个 WriteKindRollback 记录
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		return &kvrpcpb.CheckTxnStatusResponse{
			LockTtl: 0,
			Action:  kvrpcpb.Action_LockNotExistRollback,
		}, nil
	}

	// 3. 检查 Lock 是否属于这个事务
	if lock.Ts != req.GetLockTs() {
		// Lock 不属于这个事务
		return &kvrpcpb.CheckTxnStatusResponse{
			LockTtl: 0,
			Action:  kvrpcpb.Action_NoAction,
		}, nil
	}

	// 4. 检查 TTL 是否超时
	// 使用物理时间比较: lock.Ts + lock.Ttl 与 current_ts
	lockPhysicalTime := mvcc.PhysicalTime(lock.Ts)
	currentPhysicalTime := mvcc.PhysicalTime(req.GetCurrentTs())

	if lockPhysicalTime+lock.Ttl <= currentPhysicalTime {
		// TTL 超时，需要回滚这个 lock
		txn.DeleteLock(req.GetPrimaryKey())
		txn.DeleteValue(req.GetPrimaryKey())
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		return &kvrpcpb.CheckTxnStatusResponse{
			LockTtl: 0,
			Action:  kvrpcpb.Action_TTLExpireRollback,
		}, nil
	}

	// 5. Lock 存在且未超时，返回剩余 TTL
	remainingTtl := lockPhysicalTime + lock.Ttl - currentPhysicalTime
	return &kvrpcpb.CheckTxnStatusResponse{
		LockTtl: remainingTtl,
		Action:  kvrpcpb.Action_NoAction,
	}, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	start_ts := req.GetStartVersion()
	txn := mvcc.NewMvccTxn(reader, start_ts)

	for _, key := range req.Keys {
		// 检查是否已经有 Write 记录
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				// 事务已经被 commit 了，不能再回滚
				return &kvrpcpb.BatchRollbackResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "transaction already committed",
					},
				}, nil
			}
			// 如果已经是 rollback，跳过这个 key，继续处理下一个
			continue
		}

		// 检查是否有 Lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock != nil {
			// 检查 lock 是否属于当前事务
			if lock.Ts == start_ts {
				// 属于当前事务，移除 lock 和 value
				txn.DeleteLock(key)
				txn.DeleteValue(key)
			}
			// 如果不属于当前事务，不做任何操作
		}

		// 无论如何都要创建一个 rollback 记录
		txn.PutWrite(key, start_ts, &mvcc.Write{
			StartTS: start_ts,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	// 写入所有修改
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	start_ts := req.GetStartVersion()
	commit_ts := req.GetCommitVersion()

	// 如果 start_ts 为 0，说明这是一个空的 resolve 请求，直接返回
	if start_ts == 0 {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}

	txn := mvcc.NewMvccTxn(reader, start_ts)

	// 获取该事务的所有锁
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}

	if commit_ts == 0 {
		// 回滚所有锁
		for _, lockPair := range locks {
			key := lockPair.Key

			// 检查是否已经有 Write 记录
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}

			if write != nil {
				// 已经有 write 记录，跳过
				continue
			}

			// 移除锁和值
			txn.DeleteLock(key)
			txn.DeleteValue(key)

			// 创建 rollback 记录
			txn.PutWrite(key, start_ts, &mvcc.Write{
				StartTS: start_ts,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	} else {
		// 提交所有锁
		for _, lockPair := range locks {
			key := lockPair.Key
			lock := lockPair.Lock

			// 检查是否已经有 Write 记录
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}

			if write != nil {
				// 已经有 write 记录，跳过
				continue
			}

			// 提交锁
			txn.PutWrite(key, commit_ts, &mvcc.Write{
				StartTS: start_ts,
				Kind:    lock.Kind,
			})
			txn.DeleteLock(key)
		}
	}

	// 写入所有修改
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.ResolveLockResponse{}, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
