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

// PeerConfig predstavlja peer control unit (seed/discovery in metadata).
type PeerConfig struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

// RaftConfig je konfiguracija za RaftServer.
type RaftConfig struct {
	NodeID      string
	RaftAddr    string
	GRPCAddr    string
	DataDir     string
	Bootstrap   bool
	Peers       []PeerConfig // seed peers (za auto-join / fallback)
	HBTimeout   time.Duration
	TokenSecret string // za respawner
}

// RaftServer je control unit z Raft konsenzom.
type RaftServer struct {
	controlpb.UnimplementedControlPlaneServiceServer
	publicpb.UnimplementedControlPlaneServer

	mu sync.Mutex

	raft      *raft.Raft
	fsm       *FSM
	transport *raft.NetworkTransport

	nodeID   string
	raftAddr string
	grpcAddr string

	seedPeers        []PeerConfig
	hasExistingState bool

	// Heartbeat tracking (lokalno, samo leader).
	lastHB    map[string]time.Time
	hbTimeout time.Duration

	// grpc clients (za UpdateNeighbors in forward).
	conns map[string]*grpc.ClientConn
	cps   map[string]controlpb.ControlPlaneServiceClient

	// Za detekcijo menjave leaderja.
	wasLeader bool

	// Respawner za avtomatski ponovni zagon propadlih node-ov.
	respawner   *Respawner
	tokenSecret string

	// cache: node_id -> port (primarno za respawning).
	nodePorts map[string]string
}

func NewRaftServer(cfg RaftConfig) (*RaftServer, error) {
	if cfg.HBTimeout <= 0 {
		cfg.HBTimeout = 2 * time.Second // Isto kot legacy controlplane
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data/" + cfg.NodeID
	}
	if cfg.TokenSecret == "" {
		cfg.TokenSecret = "devsecret"
	}

	s := &RaftServer{
		nodeID:      cfg.NodeID,
		raftAddr:    cfg.RaftAddr,
		grpcAddr:    cfg.GRPCAddr,
		seedPeers:   cfg.Peers,
		lastHB:      make(map[string]time.Time),
		hbTimeout:   cfg.HBTimeout,
		conns:       make(map[string]*grpc.ClientConn),
		cps:         make(map[string]controlpb.ControlPlaneServiceClient),
		tokenSecret: cfg.TokenSecret,
		nodePorts:   make(map[string]string),
	}

	if err := s.setupRaft(cfg); err != nil {
		return nil, fmt.Errorf("napaka pri setup raft: %w", err)
	}

	// Poskrbi, da bo leader v log zapisal tudi svoj gRPC/raft naslov (da followerji znajo najti leaderja).
	go s.ensureSelfPeerRegistered()

	return s, nil
}

func (s *RaftServer) setupRaft(cfg RaftConfig) error {
	s.fsm = NewFSM()

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)

	rc.HeartbeatTimeout = 600 * time.Millisecond
	rc.ElectionTimeout = 600 * time.Millisecond
	rc.LeaderLeaseTimeout = 300 * time.Millisecond
	rc.CommitTimeout = 100 * time.Millisecond
	rc.SnapshotInterval = 30 * time.Second
	rc.SnapshotThreshold = 100

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("napaka pri ustvarjanju data dir: %w", err)
	}

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju bolt store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju snapshot store: %w", err)
	}

	// Preveri, ali že obstaja Raft stanje (restart).
	hasState, err := raft.HasExistingState(boltDB, boltDB, snapshotStore)
	if err != nil {
		return fmt.Errorf("napaka pri preverjanju obstoječega stanja: %w", err)
	}
	s.hasExistingState = hasState

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return fmt.Errorf("napaka pri resolve raft addr: %w", err)
	}
	bindAddr := addr.String()

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju transport: %w", err)
	}
	s.transport = transport

	r, err := raft.NewRaft(rc, s.fsm, boltDB, boltDB, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju raft: %w", err)
	}
	s.raft = r

	// Bootstrap samo na prvem node-u, in samo če ni obstoječega stanja.
	if cfg.Bootstrap && !hasState {
		future := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(bindAddr),
		}}})
		if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
			log.Printf("raft bootstrap warning: %v", err)
		}
	}

	return nil
}

func (s *RaftServer) HasExistingState() bool { return s.hasExistingState }

func (s *RaftServer) IsLeader() bool { return s.raft.State() == raft.Leader }

func (s *RaftServer) GetLeaderAddr() string {
	addr, _ := s.raft.LeaderWithID()
	return string(addr)
}

func (s *RaftServer) GetLeaderGRPCAddr() string {
	_, leaderID := s.raft.LeaderWithID()
	if leaderID == "" {
		return ""
	}
	if string(leaderID) == s.nodeID {
		return s.grpcAddr
	}
	if p, ok := s.fsm.GetPeer(string(leaderID)); ok {
		return p.GRPCAddr
	}
	for _, sp := range s.seedPeers {
		if sp.NodeID == string(leaderID) {
			return sp.GRPCAddr
		}
	}
	return ""
}

func (s *RaftServer) dial(addr string) (controlpb.ControlPlaneServiceClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("prazen naslov")
	}
	s.mu.Lock()
	if c := s.cps[addr]; c != nil {
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
	if existing := s.cps[addr]; existing != nil {
		s.mu.Unlock()
		_ = cc.Close()
		return existing, nil
	}
	s.conns[addr] = cc
	s.cps[addr] = cli
	s.mu.Unlock()
	return cli, nil
}

func (s *RaftServer) apply(cmd Command, timeout time.Duration) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(data, timeout)
	return f.Error()
}

func (s *RaftServer) ensureSelfPeerRegistered() {
	for {
		if s.raft == nil {
			return
		}
		if s.IsLeader() {
			if _, ok := s.fsm.GetPeer(s.nodeID); !ok {
				_ = s.apply(Command{Type: CommandUpsertPeer, NodeID: s.nodeID, RaftAddr: s.raftAddr, GRPCAddr: s.grpcAddr}, 3*time.Second)
				_ = s.raft.Barrier(2 * time.Second).Error()
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// AutoJoin poskuša dodati ta node v obstoječi Raft cluster.
// Seed-i so gRPC naslovi control-plane node-ov (lahko followerji ali leader).
func (s *RaftServer) AutoJoin(ctx context.Context) {
	if s.hasExistingState {
		return
	}
	if len(s.seedPeers) == 0 {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		for _, sp := range s.seedPeers {
			addr := sp.GRPCAddr
			cli, err := s.dial(addr)
			if err != nil {
				continue
			}
			lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			resp, err := cli.AddPeer(lctx, &controlpb.AddPeerRequest{NodeId: s.nodeID, RaftAddr: s.raftAddr, GrpcAddr: s.grpcAddr})
			cancel()
			if err == nil && resp != nil && resp.GetOk() {
				log.Printf("control %s: uspešno sem se pridružil raft clusterju prek %s", s.nodeID, addr)
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ---- Data-plane control RPCji ----

// Heartbeat: followerji forwardajo na leaderja, da leader ne odstrani data-node-a.
func (s *RaftServer) Heartbeat(ctx context.Context, req *controlpb.HeartbeatRequest) (*controlpb.HeartbeatResponse, error) {
	id := req.GetNodeId()
	if id == "" {
		return &controlpb.HeartbeatResponse{Ok: false}, nil
	}
	if !s.IsLeader() {
		leader := s.GetLeaderGRPCAddr()
		if leader == "" {
			return &controlpb.HeartbeatResponse{Ok: false, LeaderAddr: ""}, nil
		}
		cli, err := s.dial(leader)
		if err != nil {
			return &controlpb.HeartbeatResponse{Ok: false, LeaderAddr: leader}, nil
		}
		resp, err := cli.Heartbeat(ctx, req)
		if err != nil {
			return &controlpb.HeartbeatResponse{Ok: false, LeaderAddr: leader}, nil
		}
		return resp, nil
	}

	s.mu.Lock()
	s.lastHB[id] = time.Now()
	s.mu.Unlock()
	return &controlpb.HeartbeatResponse{Ok: true}, nil
}

// JoinChain: piše samo leader; followerji forwardajo.
func (s *RaftServer) JoinChain(ctx context.Context, req *controlpb.JoinChainRequest) (*controlpb.JoinChainResponse, error) {
	n := req.GetNode()
	if n == nil || n.GetNodeId() == "" || n.GetAddress() == "" {
		return nil, fmt.Errorf("JoinChain: neveljaven node")
	}
	if !s.IsLeader() {
		leader := s.GetLeaderGRPCAddr()
		if leader == "" {
			return nil, fmt.Errorf("nisem leader in leader ni znan")
		}
		cli, err := s.dial(leader)
		if err != nil {
			return nil, fmt.Errorf("nisem leader, poskusi na: %s", leader)
		}
		return cli.JoinChain(ctx, req)
	}

	if err := s.apply(Command{Type: CommandJoin, NodeID: n.GetNodeId(), Addr: n.GetAddress()}, 5*time.Second); err != nil {
		return nil, fmt.Errorf("raft apply error: %w", err)
	}

	// Shranjujemo porte za respawner.
	port := extractPort(n.GetAddress())

	// Ob pridružitvi šte... (leader only).
	s.mu.Lock()
	s.lastHB[n.GetNodeId()] = time.Now()
	if port != "" {
		s.nodePorts[n.GetNodeId()] = port
	}
	s.mu.Unlock()

	// Registracija node-a pri respawnerju.
	if s.respawner != nil && port != "" {
		s.respawner.RegisterNode(n.GetNodeId(), port)
	}

	idx := s.fsm.NodeIndex(n.GetNodeId())
	chain := s.fsm.GetChain()
	var prev, next *controlpb.NodeInfo
	if idx > 0 && idx < len(chain) {
		prev = chain[idx-1]
	}
	if idx >= 0 && idx+1 < len(chain) {
		next = chain[idx+1]
	}

	go s.broadcastNeighbors()
	return &controlpb.JoinChainResponse{Previous: prev, Next: next, SyncFrom: prev}, nil
}

func (s *RaftServer) GetChain(ctx context.Context, _ *controlpb.GetChainRequest) (*controlpb.GetChainResponse, error) {
	chain := s.fsm.GetChain()
	return &controlpb.GetChainResponse{Chain: chain, Head: s.fsm.GetHead(), Tail: s.fsm.GetTail()}, nil
}

// ---- Public API (client side) ----

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

// ---- Raft peer management RPCji ----

func (s *RaftServer) AddPeer(ctx context.Context, req *controlpb.AddPeerRequest) (*controlpb.AddPeerResponse, error) {
	if req.GetNodeId() == "" || req.GetRaftAddr() == "" || req.GetGrpcAddr() == "" {
		return &controlpb.AddPeerResponse{Ok: false, Error: "neveljaven AddPeerRequest"}, nil
	}

	if !s.IsLeader() {
		leader := s.GetLeaderGRPCAddr()
		if leader != "" {
			cli, err := s.dial(leader)
			if err == nil {
				return cli.AddPeer(ctx, req)
			}
		}
		return &controlpb.AddPeerResponse{Ok: false, Error: fmt.Sprintf("nisem leader, poskusi na: %s", leader)}, nil
	}

	raftAddr := req.GetRaftAddr()
	if a, err := net.ResolveTCPAddr("tcp", raftAddr); err == nil {
		raftAddr = a.String()
	}

	future := s.raft.AddVoter(raft.ServerID(req.GetNodeId()), raft.ServerAddress(raftAddr), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return &controlpb.AddPeerResponse{Ok: false, Error: err.Error()}, nil
	}

	// Replikiraj meta podatke peerja (node_id -> grpc_addr), da followerji znajo redirectat.
	if err := s.apply(Command{Type: CommandUpsertPeer, NodeID: req.GetNodeId(), RaftAddr: raftAddr, GRPCAddr: req.GetGrpcAddr()}, 5*time.Second); err != nil {
		return &controlpb.AddPeerResponse{Ok: false, Error: err.Error()}, nil
	}
	_ = s.raft.Barrier(2 * time.Second).Error()

	log.Printf("control: peer %s dodan (raft=%s grpc=%s)", req.GetNodeId(), raftAddr, req.GetGrpcAddr())
	return &controlpb.AddPeerResponse{Ok: true}, nil
}

func (s *RaftServer) RemovePeer(ctx context.Context, req *controlpb.RemovePeerRequest) (*controlpb.RemovePeerResponse, error) {
	if req.GetNodeId() == "" {
		return &controlpb.RemovePeerResponse{Ok: false, Error: "prazen node_id"}, nil
	}
	if !s.IsLeader() {
		leader := s.GetLeaderGRPCAddr()
		if leader != "" {
			cli, err := s.dial(leader)
			if err == nil {
				return cli.RemovePeer(ctx, req)
			}
		}
		return &controlpb.RemovePeerResponse{Ok: false, Error: fmt.Sprintf("nisem leader, poskusi na: %s", leader)}, nil
	}

	f := s.raft.RemoveServer(raft.ServerID(req.GetNodeId()), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return &controlpb.RemovePeerResponse{Ok: false, Error: err.Error()}, nil
	}
	_ = s.apply(Command{Type: CommandRemovePeer, NodeID: req.GetNodeId()}, 3*time.Second)
	_ = s.raft.Barrier(2 * time.Second).Error()

	log.Printf("control: peer %s odstranjen", req.GetNodeId())
	return &controlpb.RemovePeerResponse{Ok: true}, nil
}

func (s *RaftServer) GetRaftState(ctx context.Context, _ *controlpb.GetRaftStateRequest) (*controlpb.GetRaftStateResponse, error) {
	leaderAddr, leaderID := s.raft.LeaderWithID()

	// Konfiguracija iz Rafta.
	confF := s.raft.GetConfiguration()
	if err := confF.Error(); err != nil {
		return &controlpb.GetRaftStateResponse{IsLeader: s.IsLeader(), LeaderId: string(leaderID), LeaderAddr: string(leaderAddr), State: s.raft.State().String()}, nil
	}
	conf := confF.Configuration()

	peers := make([]*controlpb.RaftPeer, 0, len(conf.Servers))
	for _, srv := range conf.Servers {
		id := string(srv.ID)
		ra := string(srv.Address)
		ga := ""
		if id == s.nodeID {
			ga = s.grpcAddr
		} else if p, ok := s.fsm.GetPeer(id); ok {
			ga = p.GRPCAddr
		} else {
			for _, sp := range s.seedPeers {
				if sp.NodeID == id {
					ga = sp.GRPCAddr
					break
				}
			}
		}
		peers = append(peers, &controlpb.RaftPeer{NodeId: id, RaftAddr: ra, GrpcAddr: ga})
	}

	return &controlpb.GetRaftStateResponse{
		IsLeader:   s.IsLeader(),
		LeaderId:   string(leaderID),
		LeaderAddr: string(leaderAddr),
		Peers:      peers,
		State:      s.raft.State().String(),
	}, nil
}

func (s *RaftServer) removeNodeFromChain(nodeID string) (string, error) {
	if !s.IsLeader() {
		return "", fmt.Errorf("nisem leader")
	}

	// Poberemo port preden odstranimo.
	s.mu.Lock()
	port := s.nodePorts[nodeID]
	s.mu.Unlock()

	err := s.apply(Command{Type: CommandRemove, NodeID: nodeID}, 5*time.Second)
	return port, err
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
		cli, err := s.dial(self.GetAddress())
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		_, _ = cli.UpdateNeighbors(cctx, &controlpb.UpdateNeighborsRequest{Self: self, Previous: prev, Next: next})
		cancel()
	}
}

func (s *RaftServer) monitorFailures() {
	t := time.NewTicker(500 * time.Millisecond) // Isto kot legacy controlplane
	defer t.Stop()
	for range t.C {
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
			last, ok := s.lastHB[n.GetNodeId()]
			if !ok {
				// še ni prejel HB po menjavi leaderja -> ne odstranjuj
				continue
			}
			if now.Sub(last) > s.hbTimeout {
				dead = append(dead, n.GetNodeId())
			}
		}
		s.mu.Unlock()

		for _, id := range dead {
			log.Printf("control: node %s timeout -> odstranim iz verige", id)
			port, err := s.removeNodeFromChain(id)
			if err != nil {
				log.Printf("control: napaka pri odstranjevanju node %s: %v", id, err)
				continue
			}

			// Proži respawn request.
			if s.respawner != nil && port != "" {
				s.respawner.RequestRespawn(id, port)
			}
		}
		if len(dead) > 0 {
			go s.broadcastNeighbors()
		}
	}
}

// observeLeaderChanges preverja, ali smo postali leader in obvesti data node-e.
func (s *RaftServer) observeLeaderChanges() {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		isLeader := s.IsLeader()

		s.mu.Lock()
		wasLeader := s.wasLeader
		if isLeader && !wasLeader {
			// Pravkar smo postali leader!
			s.wasLeader = true
			s.mu.Unlock()

			log.Printf("control %s: postal sem leader, obveščam data node-e", s.nodeID)
			go s.broadcastLeaderChange()
		} else {
			s.wasLeader = isLeader
			s.mu.Unlock()
		}
	}
}

// broadcastLeaderChange obvesti vse data node-e v verigi, da smo novi leader.
func (s *RaftServer) broadcastLeaderChange() {
	// Počakaj, da se raft stabilizira.
	time.Sleep(300 * time.Millisecond)

	chain := s.fsm.GetChain()
	if len(chain) == 0 {
		return
	}

	// Zberemo vse control unit naslove (za failover).
	allAddrs := s.getAllControlAddrs()

	for _, node := range chain {
		if node == nil || node.GetAddress() == "" {
			continue
		}
		cli, err := s.dial(node.GetAddress())
		if err != nil {
			log.Printf("control %s: ne morem obvestiti %s o spremembi leaderja: %v", s.nodeID, node.GetNodeId(), err)
			continue
		}
		cctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		_, err = cli.NotifyLeaderChange(cctx, &controlpb.NotifyLeaderChangeRequest{
			LeaderId:        s.nodeID,
			LeaderGrpcAddr:  s.grpcAddr,
			AllControlAddrs: allAddrs,
		})
		cancel()
		if err != nil {
			log.Printf("control %s: NotifyLeaderChange za %s neuspešen: %v", s.nodeID, node.GetNodeId(), err)
		} else {
			log.Printf("control %s: obvestil %s o spremembi leaderja", s.nodeID, node.GetNodeId())
		}
	}

	// Po obvestilu še pošljemo neighbor update, da se data node-i resinkajo.
	go s.broadcastNeighbors()
}

// getAllControlAddrs vrne vse gRPC naslove control unit-ov (peers + self).
func (s *RaftServer) getAllControlAddrs() []string {
	addrs := make([]string, 0, len(s.seedPeers)+1)
	// Najprej dodamo sebe (leader).
	addrs = append(addrs, s.grpcAddr)

	// Dodamo vse peer-e iz Raft konfiguracije.
	confF := s.raft.GetConfiguration()
	if err := confF.Error(); err == nil {
		for _, srv := range confF.Configuration().Servers {
			id := string(srv.ID)
			if id == s.nodeID {
				continue
			}
			// Poiščemo gRPC naslov za ta peer.
			if p, ok := s.fsm.GetPeer(id); ok && p.GRPCAddr != "" {
				addrs = append(addrs, p.GRPCAddr)
			} else {
				// Fallback na seedPeers.
				for _, sp := range s.seedPeers {
					if sp.NodeID == id && sp.GRPCAddr != "" {
						addrs = append(addrs, sp.GRPCAddr)
						break
					}
				}
			}
		}
	}
	return addrs
}

// Serve zažene gRPC strežnik.
func (s *RaftServer) Serve() error {
	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return err
	}

	// Zaženemo respawner.
	s.respawner = NewRespawner(s.grpcAddr, s.tokenSecret)
	s.respawner.Start()

	gs := grpc.NewServer()

	// health
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, hs)

	controlpb.RegisterControlPlaneServiceServer(gs, s)
	publicpb.RegisterControlPlaneServer(gs, s)
	reflection.Register(gs)

	go s.monitorFailures()
	go s.observeLeaderChanges()

	log.Printf("Control unit %s zagnan (gRPC: %s, Raft: %s)", s.nodeID, s.grpcAddr, s.raftAddr)
	log.Printf("control: respawner omogočen (workers=%d, queue=%d, max_attempts=%d)",
		respawnWorkers, respawnQueueSize, maxRespawnAttempts)

	return gs.Serve(lis)
}

func (s *RaftServer) Shutdown() error {
	// Ustavimo respawner.
	if s.respawner != nil {
		s.respawner.Stop()
	}

	if s.raft != nil {
		f := s.raft.Shutdown()
		if err := f.Error(); err != nil {
			return err
		}
	}
	if s.transport != nil {
		s.transport.Close()
	}
	s.mu.Lock()
	for _, cc := range s.conns {
		_ = cc.Close()
	}
	s.conns = make(map[string]*grpc.ClientConn)
	s.cps = make(map[string]controlpb.ControlPlaneServiceClient)
	s.mu.Unlock()
	return nil
}
