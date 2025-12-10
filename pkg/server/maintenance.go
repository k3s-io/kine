package server

import (
	"context"
	"errors"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// explicit interface check
var _ etcdserverpb.MaintenanceServer = (*KVServerBridge)(nil)

func (s *KVServerBridge) Alarm(context.Context, *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	return nil, errors.New("alarm is not supported")
}

func (s *KVServerBridge) Status(ctx context.Context, r *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	size, err := s.limited.dbSize(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.StatusResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		DbSize:  size,
		Version: s.emulatedETCDVersion,
	}, nil
}

func (s *KVServerBridge) Defragment(context.Context, *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	return nil, errors.New("defragment is not supported")
}

func (s *KVServerBridge) Hash(context.Context, *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	return nil, errors.New("hash is not supported")
}

func (s *KVServerBridge) HashKV(context.Context, *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	return nil, errors.New("hash kv is not supported")
}

func (s *KVServerBridge) Snapshot(*etcdserverpb.SnapshotRequest, etcdserverpb.Maintenance_SnapshotServer) error {
	return errors.New("snapshot is not supported")
}

func (s *KVServerBridge) MoveLeader(context.Context, *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	return nil, errors.New("move leader is not supported")
}

func (s *KVServerBridge) Downgrade(context.Context, *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	return nil, errors.New("downgrade is not supported")
}
