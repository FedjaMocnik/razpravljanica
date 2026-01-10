package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// RaftServer je control unit z Raft konsenzom
type RaftServer struct {
	controlpb.UnimplementedControlPlaneServiceServer
	publicpb.UnimplementedControlPlaneServer

	mu sync.Mutex

	// Raft komponente
	raft      *raft.Raft
	fsm       *FSM
	transport *raft.NetworkTransport

	// Node identifikacija
	nodeID   string
	raftAddr string
	grpcAddr string

	// Heartbeat tracking (lokalno)
	lastHB    map[string]time.Time
	hbTimeout time.Duration

	// grpc clients to nodes (za UpdateNeighbors)
	conns map[string]*grpc.ClientConn
	cps   map[string]controlpb.ControlPlaneServiceClient

	// Peers za discovery
	peers []PeerConfig
}

// PeerConfig predstavlja peer control unit
type PeerConfig struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

// RaftConfig je konfiguracija za RaftServer
type RaftConfig struct {
	NodeID    string
	RaftAddr  string
	GRPCAddr  string
	DataDir   string
	Bootstrap bool
	Peers     []PeerConfig
	HBTimeout time.Duration
}

// NewRaftServer ustvari nov RaftServer
func NewRaftServer(cfg RaftConfig) (*RaftServer, error) {
	if cfg.HBTimeout <= 0 {
		cfg.HBTimeout = 30 * time.Second // Povečan timeout za data serverje
	}

	s := &RaftServer{
		nodeID:    cfg.NodeID,
		raftAddr:  cfg.RaftAddr,
		grpcAddr:  cfg.GRPCAddr,
		lastHB:    make(map[string]time.Time),
		hbTimeout: cfg.HBTimeout,
		conns:     make(map[string]*grpc.ClientConn),
		cps:       make(map[string]controlpb.ControlPlaneServiceClient),
		peers:     cfg.Peers,
	}

	// Inicializiraj Raft
	if err := s.setupRaft(cfg); err != nil {
		return nil, fmt.Errorf("napaka pri setup raft: %w", err)
	}

	return s, nil
}

func (s *RaftServer) setupRaft(cfg RaftConfig) error {
	// Ustvari FSM
	s.fsm = NewFSM()

	// Raft konfiguracija
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.SnapshotThreshold = 100

	// POMEMBNO: Zmanjšaj election timeout za hitrejši failover
	// Default je 1000ms heartbeat, 1000-2000ms election timeout
	raftConfig.HeartbeatTimeout = 500 * time.Millisecond
	raftConfig.ElectionTimeout = 500 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 250 * time.Millisecond
	raftConfig.CommitTimeout = 100 * time.Millisecond

	// Omogoči več logiranja za debugging
	raftConfig.LogLevel = "DEBUG"

	// Ustvari data dir
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("napaka pri ustvarjanju data dir: %w", err)
	}

	// Log store in stable store (BoltDB)
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju bolt store: %w", err)
	}

	// Snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju snapshot store: %w", err)
	}

	// Transport - POMEMBNO: uporabi resolvan IP naslov za konsistentnost
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return fmt.Errorf("napaka pri resolve addr: %w", err)
	}

	// Uporabi IP naslov namesto hostname za transport
	bindAddr := addr.String()
	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju transport: %w", err)
	}
	s.transport = transport

	// Ustvari Raft
	r, err := raft.NewRaft(raftConfig, s.fsm, boltDB, boltDB, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju raft: %w", err)
	}
	s.raft = r

	// Bootstrap če je to prvi node
	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(cfg.RaftAddr),
			},
		}

		// Dodaj peers če obstajajo
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.NodeID),
				Address: raft.ServerAddress(p.RaftAddr),
			})
		}

		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
			log.Printf("raft bootstrap warning: %v", err)
		}
	}

	return nil
}

// IsLeader vrne true če je ta node leader
func (s *RaftServer) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// GetLeaderAddr vrne naslov leaderja
func (s *RaftServer) GetLeaderAddr() string {
	addr, _ := s.raft.LeaderWithID()
	return string(addr)
}

// GetLeaderGRPCAddr vrne gRPC naslov leaderja
func (s *RaftServer) GetLeaderGRPCAddr() string {
	_, leaderID := s.raft.LeaderWithID()
	if leaderID == "" {
		return ""
	}

	// Če smo mi leader
	if string(leaderID) == s.nodeID {
		return s.grpcAddr
	}

	// Poišči med peeri
	for _, p := range s.peers {
		if p.NodeID == string(leaderID) {
			return p.GRPCAddr
		}
	}
	return ""
}

// WaitForReplication počaka, da so vsi peerji sinhronizirani
// Vrne true če je replikacija uspešna, false če timeout
func (s *RaftServer) WaitForReplication(timeout time.Duration) bool {
	if !s.IsLeader() {
		return false
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// Barrier zagotovi, da so vsi commiti replicirani
		future := s.raft.Barrier(time.Second)
		if future.Error() == nil {
			log.Printf("Replikacija uspešna - vsi peerji sinhronizirani")
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("Replikacija timeout po %v", timeout)
	return false
}

// GetReplicationStatus vrne status replikacije za vsakega peerja
func (s *RaftServer) GetReplicationStatus() map[string]uint64 {
	stats := s.raft.Stats()
	result := make(map[string]uint64)

	// Zadnji commit index
	if idx, ok := stats["commit_index"]; ok {
		result["commit_index"] = parseUint64(idx)
	}

	// Zadnji applied index
	if idx, ok := stats["applied_index"]; ok {
		result["applied_index"] = parseUint64(idx)
	}

	// Zadnji log index
	if idx, ok := stats["last_log_index"]; ok {
		result["last_log_index"] = parseUint64(idx)
	}

	return result
}

func parseUint64(s string) uint64 {
	var v uint64
	fmt.Sscanf(s, "%d", &v)
	return v
}

func (s *RaftServer) dialNode(addr string) (controlpb.ControlPlaneServiceClient, error) {
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

// Heartbeat - samo leader shranjuje heartbeat
func (s *RaftServer) Heartbeat(ctx context.Context, req *controlpb.HeartbeatRequest) (*controlpb.HeartbeatResponse, error) {
	id := req.GetNodeId()
	if id == "" {
		return &controlpb.HeartbeatResponse{Ok: false}, nil
	}

	// Če nismo leader, preusmeri
	if !s.IsLeader() {
		return &controlpb.HeartbeatResponse{Ok: true, LeaderAddr: s.GetLeaderGRPCAddr()}, nil
	}

	s.mu.Lock()
	s.lastHB[id] = time.Now()
	s.mu.Unlock()
	return &controlpb.HeartbeatResponse{Ok: true}, nil
}

// JoinChain - samo leader lahko spreminja verigo
func (s *RaftServer) JoinChain(ctx context.Context, req *controlpb.JoinChainRequest) (*controlpb.JoinChainResponse, error) {
	n := req.GetNode()
	if n == nil || n.GetNodeId() == "" || n.GetAddress() == "" {
		return nil, fmt.Errorf("JoinChain: neveljaven node")
	}

	// Če nismo leader, vrni error z naslovom leaderja
	if !s.IsLeader() {
		return nil, fmt.Errorf("nisem leader, poskusi na: %s", s.GetLeaderGRPCAddr())
	}

	// Ustvari ukaz
	cmd := Command{
		Type:   CommandJoin,
		NodeID: n.GetNodeId(),
		Addr:   n.GetAddress(),
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("napaka pri marshal: %w", err)
	}

	// Apliciraj preko Raft
	future := s.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply error: %w", err)
	}

	// Posodobi heartbeat
	s.mu.Lock()
	s.lastHB[n.GetNodeId()] = time.Now()
	s.mu.Unlock()

	// Pridobi sosede iz FSM
	idx := s.fsm.NodeIndex(n.GetNodeId())
	chain := s.fsm.GetChain()

	var prev, next *controlpb.NodeInfo
	if idx > 0 && idx < len(chain) {
		prev = chain[idx-1]
	}
	if idx >= 0 && idx+1 < len(chain) {
		next = chain[idx+1]
	}

	// Broadcast neighbors
	go s.broadcastNeighbors()

	return &controlpb.JoinChainResponse{Previous: prev, Next: next, SyncFrom: prev}, nil
}

// GetChain vrne trenutno verigo
func (s *RaftServer) GetChain(ctx context.Context, _ *controlpb.GetChainRequest) (*controlpb.GetChainResponse, error) {
	chain := s.fsm.GetChain()
	return &controlpb.GetChainResponse{
		Chain: chain,
		Head:  s.fsm.GetHead(),
		Tail:  s.fsm.GetTail(),
	}, nil
}

// GetClusterState - public API za odjemalce
func (s *RaftServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*publicpb.GetClusterStateResponse, error) {
	head := s.fsm.GetHead()
	tail := s.fsm.GetTail()

	var h, t *publicpb.NodeInfo
	if head != nil {
		h = &publicpb.NodeInfo{NodeId: head.GetNodeId(), Address: head.GetAddress()}
	}
	if tail != nil {
		t = &publicpb.NodeInfo{NodeId: tail.GetNodeId(), Address: tail.GetAddress()}
	}
	return &publicpb.GetClusterStateResponse{Head: h, Tail: t}, nil
}

func (s *RaftServer) removeNodeFromChain(nodeID string) error {
	if !s.IsLeader() {
		return fmt.Errorf("nisem leader")
	}

	cmd := Command{
		Type:   CommandRemove,
		NodeID: nodeID,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := s.raft.Apply(data, 5*time.Second)
	return future.Error()
}

func (s *RaftServer) broadcastNeighbors() {
	chain := s.fsm.GetChain()

	for i, self := range chain {
		var prev, next *controlpb.NodeInfo
		if i > 0 {
			prev = chain[i-1]
		}
		if i+1 < len(chain) {
			next = chain[i+1]
		}

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
	}
}

func (s *RaftServer) monitorFailures() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		// Samo leader monitora
		if !s.IsLeader() {
			continue
		}

		now := time.Now()
		chain := s.fsm.GetChain()

		var dead []string
		s.mu.Lock()
		for _, n := range chain {
			if n == nil {
				continue
			}
			t, ok := s.lastHB[n.GetNodeId()]
			if !ok {
				// Node še ni poslal heartbeata - preskočimo (šele se je pridružil)
				continue
			}
			// Če je timeout potekel in je node vsaj enkrat poslal heartbeat
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
			if err := s.removeNodeFromChain(id); err != nil {
				log.Printf("control: napaka pri odstranjevanju %s: %v", id, err)
			}
		}
		s.broadcastNeighbors()
	}
}

// Serve zažene gRPC strežnik
func (s *RaftServer) Serve() error {
	lis, err := net.Listen("tcp", s.grpcAddr)
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

	log.Printf("Control unit %s zagnan (gRPC: %s, Raft: %s)", s.nodeID, s.grpcAddr, s.raftAddr)
	return gs.Serve(lis)
}

// Shutdown ustavi strežnik
func (s *RaftServer) Shutdown() error {
	if s.raft != nil {
		future := s.raft.Shutdown()
		if err := future.Error(); err != nil {
			return err
		}
	}
	if s.transport != nil {
		s.transport.Close()
	}
	return nil
}

// GetRaftStats vrne statistiko Raft
func (s *RaftServer) GetRaftStats() map[string]string {
	return s.raft.Stats()
}

// AddPeer RPC - doda nov peer v Raft cluster
func (s *RaftServer) AddPeer(ctx context.Context, req *controlpb.AddPeerRequest) (*controlpb.AddPeerResponse, error) {
	if !s.IsLeader() {
		return &controlpb.AddPeerResponse{Ok: false, Error: fmt.Sprintf("nisem leader, poskusi na: %s", s.GetLeaderGRPCAddr())}, nil
	}

	// Uporabi konsistenten IP naslov (resolve hostname)
	raftAddr := req.GetRaftAddr()
	if addr, err := net.ResolveTCPAddr("tcp", raftAddr); err == nil {
		raftAddr = addr.String() // Uporabi resolved IP
	}

	log.Printf("Dodajam peer %s z Raft naslovom %s", req.GetNodeId(), raftAddr)

	future := s.raft.AddVoter(raft.ServerID(req.GetNodeId()), raft.ServerAddress(raftAddr), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return &controlpb.AddPeerResponse{Ok: false, Error: err.Error()}, nil
	}

	// Shrani peer info
	s.mu.Lock()
	s.peers = append(s.peers, PeerConfig{
		NodeID:   req.GetNodeId(),
		RaftAddr: raftAddr,
		GRPCAddr: req.GetGrpcAddr(),
	})
	s.mu.Unlock()

	// Počakaj, da se replikacija zaključi
	log.Printf("Čakam na replikacijo za peer %s...", req.GetNodeId())
	if !s.WaitForReplication(5 * time.Second) {
		log.Printf("OPOZORILO: replikacija za %s še ni končana", req.GetNodeId())
	}

	log.Printf("Peer %s (%s) dodan v cluster", req.GetNodeId(), raftAddr)
	return &controlpb.AddPeerResponse{Ok: true}, nil
}

// RemovePeer RPC - odstrani peer iz Raft cluster
func (s *RaftServer) RemovePeer(ctx context.Context, req *controlpb.RemovePeerRequest) (*controlpb.RemovePeerResponse, error) {
	if !s.IsLeader() {
		return &controlpb.RemovePeerResponse{Ok: false, Error: fmt.Sprintf("nisem leader, poskusi na: %s", s.GetLeaderGRPCAddr())}, nil
	}

	future := s.raft.RemoveServer(raft.ServerID(req.GetNodeId()), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return &controlpb.RemovePeerResponse{Ok: false, Error: err.Error()}, nil
	}

	// Odstrani peer info
	s.mu.Lock()
	for i, p := range s.peers {
		if p.NodeID == req.GetNodeId() {
			s.peers = append(s.peers[:i], s.peers[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	log.Printf("Peer %s odstranjen iz clusterja", req.GetNodeId())
	return &controlpb.RemovePeerResponse{Ok: true}, nil
}

// GetRaftState RPC - vrne stanje Raft clusterja
func (s *RaftServer) GetRaftState(ctx context.Context, req *controlpb.GetRaftStateRequest) (*controlpb.GetRaftStateResponse, error) {
	leaderAddr, leaderID := s.raft.LeaderWithID()

	peers := make([]*controlpb.RaftPeer, 0, len(s.peers)+1)

	// Dodaj sebe
	peers = append(peers, &controlpb.RaftPeer{
		NodeId:   s.nodeID,
		RaftAddr: s.raftAddr,
		GrpcAddr: s.grpcAddr,
	})

	// Dodaj ostale peer-e
	s.mu.Lock()
	for _, p := range s.peers {
		peers = append(peers, &controlpb.RaftPeer{
			NodeId:   p.NodeID,
			RaftAddr: p.RaftAddr,
			GrpcAddr: p.GRPCAddr,
		})
	}
	s.mu.Unlock()

	state := s.raft.State().String()

	return &controlpb.GetRaftStateResponse{
		IsLeader:   s.IsLeader(),
		LeaderId:   string(leaderID),
		LeaderAddr: string(leaderAddr),
		Peers:      peers,
		State:      state,
	}, nil
}
