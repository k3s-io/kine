package server

import (
	"context"
	"errors"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// explicit interface check
var _ etcdserverpb.LeaseServer = (*KVServerBridge)(nil)

func (s *KVServerBridge) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return &etcdserverpb.LeaseGrantResponse{
		Header: &etcdserverpb.ResponseHeader{},
		ID:     req.TTL,
		TTL:    req.TTL,
	}, nil
}

func (s *KVServerBridge) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return nil, errors.New("lease revoke is not supported")
}

func (s *KVServerBridge) LeaseKeepAlive(etcdserverpb.Lease_LeaseKeepAliveServer) error {
	return errors.New("lease keep alive is not supported")
}

func (s *KVServerBridge) LeaseTimeToLive(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return nil, errors.New("lease time to live is not supported")
}

func (s *KVServerBridge) LeaseLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return nil, errors.New("lease leases is not supported")
}
