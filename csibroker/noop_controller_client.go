package csibroker

import (
	"golang.org/x/net/context"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"google.golang.org/grpc"
)

type NoopControllerClient struct{}

func (c *NoopControllerClient) CreateVolume(ctx context.Context, in *csi.CreateVolumeRequest, opts ...grpc.CallOption) (*csi.CreateVolumeResponse, error) {
	var capacityBytes int64
	if in.CapacityRange != nil {
		capacityBytes = in.CapacityRange.RequiredBytes
	}
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: capacityBytes,
			Id:            in.Name,
			Attributes:    in.Parameters,
		},
	}, nil
}

func (c *NoopControllerClient) DeleteVolume(ctx context.Context, in *csi.DeleteVolumeRequest, opts ...grpc.CallOption) (*csi.DeleteVolumeResponse, error) {
	return new(csi.DeleteVolumeResponse), nil
}

func (c *NoopControllerClient) ControllerPublishVolume(ctx context.Context, in *csi.ControllerPublishVolumeRequest, opts ...grpc.CallOption) (*csi.ControllerPublishVolumeResponse, error) {
	return new(csi.ControllerPublishVolumeResponse), nil
}

func (c *NoopControllerClient) ControllerUnpublishVolume(ctx context.Context, in *csi.ControllerUnpublishVolumeRequest, opts ...grpc.CallOption) (*csi.ControllerUnpublishVolumeResponse, error) {
	return new(csi.ControllerUnpublishVolumeResponse), nil
}

func (c *NoopControllerClient) ValidateVolumeCapabilities(ctx context.Context, in *csi.ValidateVolumeCapabilitiesRequest, opts ...grpc.CallOption) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return new(csi.ValidateVolumeCapabilitiesResponse), nil
}

func (c *NoopControllerClient) ListVolumes(ctx context.Context, in *csi.ListVolumesRequest, opts ...grpc.CallOption) (*csi.ListVolumesResponse, error) {
	return new(csi.ListVolumesResponse), nil
}

func (c *NoopControllerClient) GetCapacity(ctx context.Context, in *csi.GetCapacityRequest, opts ...grpc.CallOption) (*csi.GetCapacityResponse, error) {
	return new(csi.GetCapacityResponse), nil
}

func (c *NoopControllerClient) ControllerGetCapabilities(ctx context.Context, in *csi.ControllerGetCapabilitiesRequest, opts ...grpc.CallOption) (*csi.ControllerGetCapabilitiesResponse, error) {
	return new(csi.ControllerGetCapabilitiesResponse), nil
}

func (c *NoopControllerClient) CreateSnapshot(ctx context.Context, in *csi.CreateSnapshotRequest, opts ...grpc.CallOption) (*csi.CreateSnapshotResponse, error) {
	return new(csi.CreateSnapshotResponse), nil
}

func (c *NoopControllerClient) DeleteSnapshot(ctx context.Context, in *csi.DeleteSnapshotRequest, opts ...grpc.CallOption) (*csi.DeleteSnapshotResponse, error) {
	return new(csi.DeleteSnapshotResponse), nil
}

func (c *NoopControllerClient) ListSnapshots(ctx context.Context, in *csi.ListSnapshotsRequest, opts ...grpc.CallOption) (*csi.ListSnapshotsResponse, error) {
	return new(csi.ListSnapshotsResponse), nil
}
