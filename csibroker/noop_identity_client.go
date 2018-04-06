package csibroker

import (
	"context"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"google.golang.org/grpc"
)

type NoopIdentityClient struct{}

func (c *NoopIdentityClient) GetPluginInfo(ctx context.Context, in *csi.GetPluginInfoRequest, opts ...grpc.CallOption) (*csi.GetPluginInfoResponse, error) {
	return new(csi.GetPluginInfoResponse), nil
}

func (c *NoopIdentityClient) GetPluginCapabilities(ctx context.Context, in *csi.GetPluginCapabilitiesRequest, opts ...grpc.CallOption) (*csi.GetPluginCapabilitiesResponse, error) {
	return new(csi.GetPluginCapabilitiesResponse), nil
}

func (c *NoopIdentityClient) Probe(ctx context.Context, in *csi.ProbeRequest, opts ...grpc.CallOption) (*csi.ProbeResponse, error) {
	return new(csi.ProbeResponse), nil
}
