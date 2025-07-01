package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// write 迭代器，用于扫描 write CF
	writeIter engine_util.DBIterator
	// 事务对象，用于读取值
	txn *MvccTxn
	// 扫描器是否已经结束
	finished bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// 创建 write CF 的迭代器
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)

	scanner := &Scanner{
		writeIter: writeIter,
		txn:       txn,
		finished:  false,
	}

	// 定位到起始位置
	// 我们需要找到第一个 >= startKey 的用户 key
	// 使用最大时间戳来确保我们从正确的位置开始
	seekKey := EncodeKey(startKey, ^uint64(0))
	scanner.writeIter.Seek(seekKey)

	return scanner
}

func (scan *Scanner) Close() {
	if scan.writeIter != nil {
		scan.writeIter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	if scan.finished {
		return nil, nil, nil
	}

	for scan.writeIter.Valid() {
		item := scan.writeIter.Item()
		writeKey := item.Key()
		userKey := DecodeUserKey(writeKey)
		commitTs := decodeTimestamp(writeKey)

		// 如果这个版本在我们的时间戳范围内
		if commitTs <= scan.txn.StartTS {
			writeValue, err := item.Value()
			if err != nil {
				return nil, nil, err
			}

			write, err := ParseWrite(writeValue)
			if err != nil {
				return nil, nil, err
			}

			if write.Kind == WriteKindPut {
				// 从 default CF 读取实际的值
				valueKey := EncodeKey(userKey, write.StartTS)
				value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, valueKey)
				if err != nil {
					return nil, nil, err
				}

				// 跳到下一个不同的用户 key
				scan.skipToNextUserKey(userKey)

				// 返回这个键值对
				return userKey, value, nil
			} else {
				// 如果是删除或回滚，跳过这个 key
				scan.skipToNextUserKey(userKey)
				continue
			}
		}

		scan.writeIter.Next()
	}

	// 没有更多的条目了
	scan.finished = true
	return nil, nil, nil
}

// skipToNextUserKey 跳到下一个不同的用户 key
func (scan *Scanner) skipToNextUserKey(currentUserKey []byte) {
	for scan.writeIter.Valid() {
		scan.writeIter.Next()
		if !scan.writeIter.Valid() {
			break
		}

		item := scan.writeIter.Item()
		writeKey := item.Key()
		userKey := DecodeUserKey(writeKey)

		if !bytes.Equal(userKey, currentUserKey) {
			break
		}
	}
}
