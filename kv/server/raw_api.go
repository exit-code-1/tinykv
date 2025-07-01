package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	resp := &kvrpcpb.RawGetResponse{}
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = val
	resp.NotFound = false
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{
		Data: put,
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{
		Data: delete,
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	kv_pairs := make([]*kvrpcpb.KvPair, 0, req.GetLimit())
	for iter.Seek(req.GetStartKey()); iter.Valid(); iter.Next() {
		if uint32(len(kv_pairs)) >= req.GetLimit() {
			break
		}
		item := iter.Item()
		val, key_err := item.ValueCopy(nil)
		if key_err != nil {
			err = key_err
			break
		}
		kv := &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: val,
		}
		kv_pairs = append(kv_pairs, kv)
	}

	resp := &kvrpcpb.RawScanResponse{
		Kvs: kv_pairs,
	}
	return resp, err
}
