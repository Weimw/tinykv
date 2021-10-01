package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	res := &kvrpcpb.RawGetResponse {
		Value: value,
		NotFound: (value == nil),
	}

	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var batch []storage.Modify
	batch = append(batch, storage.Modify{Data: storage.Put{Key: req.Key, Value: req.Value, Cf:req.Cf}})
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err 
	}
	return &kvrpcpb.RawPutResponse{}, nil 
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var batch []storage.Modify
	batch = append(batch, storage.Modify{Data:storage.Delete{Key: req.Key, Cf:req.Cf}})
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err 
	}
	return &kvrpcpb.RawDeleteResponse{}, nil 
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	iter := reader.IterCF(req.Cf)
	var kvs []*kvrpcpb.KvPair

	iter.Seek(req.StartKey)

	for i := uint32(0); iter.Valid() && i < req.Limit; i++ {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		key := item.KeyCopy(nil)
		kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value:value})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs:kvs}, nil
}
