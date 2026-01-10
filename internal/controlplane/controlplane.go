package controlplane

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// Server je dedicated control unit. Sprejema Join/Heartbeat od node-ov, zazna odpoved
// na podlagi heartbeat timeouta in posodobi verigo (chain). Po vsaki spremembi
// potisne nove sosede na VSA vozlišča preko UpdateNeighbors RPC.
//
// Poleg tega implementira tudi public ControlPlane (GetClusterState), da odjemalec
// vedno lahko najde trenutno glavo in rep.
type Server struct {
	controlpb.UnimplementedControlPlaneServiceServer
	publicpb.UnimplementedControlPlaneServer

	mu sync.Mutex

	chain  []*controlpb.NodeInfo
	lastHB map[string]time.Time

	// cache: node_id -> last known prev address (za odločitev ali je potreben catch-up)
	lastPrev map[string]string

	// grpc clients to nodes (za UpdateNeighbors)
	conns map[string]*grpc.ClientConn
	cps   map[string]controlpb.ControlPlaneServiceClient

	hbTimeout time.Duration
}

func New(hbTimeout time.Duration) *Server {
	if hbTimeout <= 0 {
		hbTimeout = 30 * time.Second // Povečan timeout za data serverje
	}
	return &Server{
		chain:     []*controlpb.NodeInfo{},
		lastHB:    map[string]time.Time{},
		lastPrev:  map[string]string{},
		conns:     map[string]*grpc.ClientConn{},
		cps:       map[string]controlpb.ControlPlaneServiceClient{},
		hbTimeout: hbTimeout,
	}
}

func (s *Server) dialNode(addr string) (controlpb.ControlPlaneServiceClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("prazen naslov")
	}
	s.mu.Lock()
	if c, ok := s.cps[addr]; ok && c != nil {
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	cli := controlpb.NewControlPlaneServiceClient(cc)

	s.mu.Lock()
	if c, ok := s.cps[addr]; ok && c != nil {
		s.mu.Unlock()
		_ = cc.Close()
		return c, nil
	}
	s.conns[addr] = cc
	s.cps[addr] = cli
	s.mu.Unlock()
	return cli, nil
}

func (s *Server) nodeIndexLocked(nodeID string) int {
	for i, n := range s.chain {
		if n != nil && n.GetNodeId() == nodeID {
			return i
		}
	}
	return -1
}

func (s *Server) headLocked() *controlpb.NodeInfo {
	if len(s.chain) == 0 {
		return nil
	}
	return s.chain[0]
}

func (s *Server) tailLocked() *controlpb.NodeInfo {
	if len(s.chain) == 0 {
		return nil
	}
	return s.chain[len(s.chain)-1]
}

func (s *Server) Heartbeat(ctx context.Context, req *controlpb.HeartbeatRequest) (*controlpb.HeartbeatResponse, error) {
	id := req.GetNodeId()
	if id == "" {
		return &controlpb.HeartbeatResponse{Ok: false}, nil
	}
	s.mu.Lock()
	s.lastHB[id] = time.Now()
	s.mu.Unlock()
	return &controlpb.HeartbeatResponse{Ok: true}, nil
}

func (s *Server) JoinChain(ctx context.Context, req *controlpb.JoinChainRequest) (*controlpb.JoinChainResponse, error) {
	n := req.GetNode()
	if n == nil || n.GetNodeId() == "" || n.GetAddress() == "" {
		return nil, fmt.Errorf("JoinChain: neveljaven node")
	}

	s.mu.Lock()
	s.lastHB[n.GetNodeId()] = time.Now()

	idx := s.nodeIndexLocked(n.GetNodeId())
	if idx >= 0 {
		// Re-join / update address, ohranimo pozicijo.
		s.chain[idx] = &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()}
	} else {
		// Privzeto dodamo na rep.
		s.chain = append(s.chain, &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
		idx = len(s.chain) - 1
	}

	var prev, next *controlpb.NodeInfo
	if idx > 0 {
		prev = s.chain[idx-1]
	}
	if idx+1 < len(s.chain) {
		next = s.chain[idx+1]
	}

	s.mu.Unlock()

	// Po join-u vedno razpošljemo novo konfiguracijo.
	go s.broadcastNeighbors()

	return &controlpb.JoinChainResponse{Previous: prev, Next: next, SyncFrom: prev}, nil
}

func (s *Server) GetChain(ctx context.Context, _ *controlpb.GetChainRequest) (*controlpb.GetChainResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Kopije
	out := make([]*controlpb.NodeInfo, 0, len(s.chain))
	for _, n := range s.chain {
		if n == nil {
			continue
		}
		out = append(out, &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
	}
	return &controlpb.GetChainResponse{Chain: out, Head: s.headLocked(), Tail: s.tailLocked()}, nil
}

// Public API za odjemalce.
func (s *Server) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*publicpb.GetClusterStateResponse, error) {
	s.mu.Lock()
	head := s.headLocked()
	tail := s.tailLocked()
	s.mu.Unlock()

	// Pretvorimo v public NodeInfo.
	var h, t *publicpb.NodeInfo
	if head != nil {
		h = &publicpb.NodeInfo{NodeId: head.GetNodeId(), Address: head.GetAddress()}
	}
	if tail != nil {
		t = &publicpb.NodeInfo{NodeId: tail.GetNodeId(), Address: tail.GetAddress()}
	}
	return &publicpb.GetClusterStateResponse{Head: h, Tail: t}, nil
}

func (s *Server) removeNode(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.nodeIndexLocked(nodeID)
	if idx < 0 {
		return
	}
	// Remove from chain
	s.chain = append(s.chain[:idx], s.chain[idx+1:]...)
	delete(s.lastHB, nodeID)
}

func (s *Server) broadcastNeighbors() {
	// snapshot
	s.mu.Lock()
	chain := make([]*controlpb.NodeInfo, 0, len(s.chain))
	for _, n := range s.chain {
		if n == nil {
			continue
		}
		chain = append(chain, &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
	}
	lastPrev := make(map[string]string, len(s.lastPrev))
	for k, v := range s.lastPrev {
		lastPrev[k] = v
	}
	s.mu.Unlock()

	for i, self := range chain {
		var prev, next *controlpb.NodeInfo
		if i > 0 {
			prev = chain[i-1]
		}
		if i+1 < len(chain) {
			next = chain[i+1]
		}

		// Update cache (prev) for future decisions. Node will detect changes itself,
		// ampak cache je uporaben tudi za log.
		prevAddr := ""
		if prev != nil {
			prevAddr = prev.GetAddress()
		}
		_ = lastPrev[self.GetNodeId()] // for clarity

		cli, err := s.dialNode(self.GetAddress())
		if err != nil {
			log.Printf("control: ne morem dial %s (%s): %v", self.GetNodeId(), self.GetAddress(), err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		_, err = cli.UpdateNeighbors(ctx, &controlpb.UpdateNeighborsRequest{Self: self, Previous: prev, Next: next})
		cancel()
		if err != nil {
			log.Printf("control: UpdateNeighbors failed %s (%s): %v", self.GetNodeId(), self.GetAddress(), err)
		}

		// cache update
		s.mu.Lock()
		s.lastPrev[self.GetNodeId()] = prevAddr
		s.mu.Unlock()
	}
}

func (s *Server) monitorFailures() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()

		var dead []string
		s.mu.Lock()
		for _, n := range s.chain {
			if n == nil {
				continue
			}
			t, ok := s.lastHB[n.GetNodeId()]
			if !ok {
				continue
			}
			if now.Sub(t) > s.hbTimeout {
				dead = append(dead, n.GetNodeId())
			}
		}
		s.mu.Unlock()

		if len(dead) == 0 {
			continue
		}
		for _, id := range dead {
			log.Printf("control: node %s timeout -> odstranim iz verige", id)
			s.removeNode(id)
		}
		s.broadcastNeighbors()
	}
}

func (s *Server) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	gs := grpc.NewServer()

	// health
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, healthSrv)

	controlpb.RegisterControlPlaneServiceServer(gs, s)
	publicpb.RegisterControlPlaneServer(gs, s)

	reflection.Register(gs)

	go s.monitorFailures()
	return gs.Serve(lis)
}
