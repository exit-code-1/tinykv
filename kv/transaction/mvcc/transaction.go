package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, ts),
			Value: write.ToBytes(),
			Cf:    "write",
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// 从 lock CF 读取 key 对应的值
	value, err := txn.Reader.GetCF("lock", key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	lock, err := ParseLock(value)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    "lock",
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  "lock",
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// 遍历 write CF，查找 commitTS <= txn.StartTS 的最新 write
	iter := txn.Reader.IterCF("write")
	defer iter.Close()

	seekKey := EncodeKey(key, txn.StartTS)
	iter.Seek(seekKey)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		itemKey := item.Key()
		userKey := DecodeUserKey(itemKey)
		if !bytes.Equal(userKey, key) {
			// 已经不是同一个 user key 了，直接返回
			return nil, nil
		}
		commitTs := decodeTimestamp(itemKey)
		if commitTs > txn.StartTS {
			// 版本太新，跳过
			continue
		}
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, err
		}
		if write.Kind != WriteKindPut {
			// 不是 put 类型，说明被删除或回滚
			return nil, nil
		}
		// 从 default CF 取出 value
		defaultValue, err := txn.Reader.GetCF("default", EncodeKey(key, write.StartTS))
		if err != nil {
			return nil, err
		}
		return defaultValue, nil
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
			Cf:    "default",
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS),
			Cf:  "default",
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF("write")
	defer iter.Close()

	seekKey := EncodeKey(key, ^uint64(0))
	iter.Seek(seekKey)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		itemKey := item.Key()
		userKey := DecodeUserKey(itemKey)
		if !bytes.Equal(userKey, key) {
			// 已经不是同一个 user key 了，直接返回
			return nil, 0, nil
		}
		commitTs := decodeTimestamp(itemKey)
		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		if write.StartTS > txn.StartTS {
			// 该 write 不是本事务的，继续找
			continue
		}
		if write.StartTS == txn.StartTS {
			return write, commitTs, nil
		}
		// 已经小于 txn.StartTS，说明没有了
		return nil, 0, nil
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF("write")
	defer iter.Close()

	seekKey := EncodeKey(key, ^uint64(0))
	iter.Seek(seekKey)
	if iter.Valid() {
		item := iter.Item()
		itemKey := item.Key()
		userKey := DecodeUserKey(itemKey)
		if bytes.Equal(userKey, key) {
			commitTs := decodeTimestamp(itemKey)
			value, err := item.Value()
			if err != nil {
				return nil, 0, err
			}
			write, err := ParseWrite(value)
			if err != nil {
				return nil, 0, err
			}
			return write, commitTs, nil
		}
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
