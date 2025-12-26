package control_unit

import (
	"context"
	"sync"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/cluster"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type ControlPlaneServer struct {
	pb.UnimplementedControlPlaneServer

	glava *pb.NodeInfo
	rep   *pb.NodeInfo
	chain []*pb.NodeInfo

	mu sync.Mutex

	conns   map[string]*grpc.ClientConn
	healthC map[string]grpc_health_v1.HealthClient
}

func NewControlPlaneServer(naslov string) *ControlPlaneServer {
	vozlisce := &pb.NodeInfo{NodeId: "node1", Address: naslov}
	return &ControlPlaneServer{glava: vozlisce, rep: vozlisce, chain: []*pb.NodeInfo{vozlisce}, conns: map[string]*grpc.ClientConn{}, healthC: map[string]grpc_health_v1.HealthClient{}}
}

func NewControlPlaneServerFromChain(chain *cluster.ChainConfig) *ControlPlaneServer {
	if chain == nil || len(chain.Nodes) == 0 {
		return NewControlPlaneServer("")
	}
	return &ControlPlaneServer{glava: chain.Head(), rep: chain.Tail(), chain: chain.Nodes, conns: map[string]*grpc.ClientConn{}, healthC: map[string]grpc_health_v1.HealthClient{}}
}

func (s *ControlPlaneServer) dialHealth(addr string) (grpc_health_v1.HealthClient, error) {
	if addr == "" {
		return nil, status.Error(codes.InvalidArgument, "prazen naslov vozlišča")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.healthC[addr]; ok && c != nil {
		return c, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	s.conns[addr] = cc
	s.healthC[addr] = grpc_health_v1.NewHealthClient(cc)
	return s.healthC[addr], nil
}

func (s *ControlPlaneServer) checkServing(ctx context.Context, n *pb.NodeInfo) error {
	if n == nil || n.GetAddress() == "" {
		return status.Error(codes.Unavailable, "neveljavno vozlišče")
	}
	c, err := s.dialHealth(n.GetAddress())
	if err != nil {
		return status.Errorf(codes.Unavailable, "vozlišče %s (%s) ni dosegljivo: %v", n.GetNodeId(), n.GetAddress(), err)
	}
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	resp, err := c.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	if err != nil {
		return status.Errorf(codes.Unavailable, "vozlišče %s (%s) ni zdravo: %v", n.GetNodeId(), n.GetAddress(), err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return status.Errorf(codes.Unavailable, "vozlišče %s (%s) ni v stanju SERVING", n.GetNodeId(), n.GetAddress())
	}
	return nil
}
func (s *ControlPlaneServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {

	if err := s.checkServing(ctx, s.glava); err != nil {
		return nil, err
	}
	if err := s.checkServing(ctx, s.rep); err != nil {
		return nil, err
	}
	return &pb.GetClusterStateResponse{
		Head: s.glava,
		Tail: s.rep,
	}, nil
}
