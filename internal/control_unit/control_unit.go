package control_unit

import (
	"context"

	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type ControlPlaneServer struct {
	pb.UnimplementedControlPlaneServer

	glava *pb.NodeInfo
	rep   *pb.NodeInfo
}

func NewControlPlaneServer(naslov string) *ControlPlaneServer {
	vozlisce := &pb.NodeInfo{
		NodeId:  "node1",
		Address: naslov,
	}
	return &ControlPlaneServer{
		glava: vozlisce,
		rep:   vozlisce,
	}
}

func (s *ControlPlaneServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	return &pb.GetClusterStateResponse{
		Head: s.glava,
		Tail: s.rep,
	}, nil
}
