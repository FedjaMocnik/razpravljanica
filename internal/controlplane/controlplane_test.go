package controlplane

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServer_JoinChain(t *testing.T) {
	s := New(time.Second)
	ctx := context.Background()

	req1 := &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node1", Address: "localhost:5001"},
	}
	resp1, err := s.JoinChain(ctx, req1)
	if err != nil {
		t.Fatalf("JoinChain ni uspel: %v", err)
	}
	if resp1.Previous != nil {
		t.Error("Prvo vozlišče ne bi smelo imeti predhodnika")
	}
	if resp1.Next != nil {
		t.Error("Prvo vozlišče ne bi smelo imeti naslednika")
	}

	req2 := &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node2", Address: "localhost:5002"},
	}
	resp2, err := s.JoinChain(ctx, req2)
	if err != nil {
		t.Fatalf("JoinChain ni uspel: %v", err)
	}
	if resp2.Previous == nil || resp2.Previous.NodeId != "node1" {
		t.Error("Drugo vozlišče bi moralo imeti node1 kot predhodnika")
	}

	chainResp, err := s.GetChain(ctx, &controlpb.GetChainRequest{})
	if err != nil {
		t.Fatalf("GetChain ni uspel: %v", err)
	}
	if len(chainResp.Chain) != 2 {
		t.Errorf("Pričakovana dolžina verige 2, dobljeno %d", len(chainResp.Chain))
	}
	if chainResp.Head.NodeId != "node1" {
		t.Error("Glava bi morala biti node1")
	}
	if chainResp.Tail.NodeId != "node2" {
		t.Error("Rep bi moral biti node2")
	}
}

func TestServer_Heartbeat(t *testing.T) {
	s := New(time.Second)
	ctx := context.Background()

	s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node1", Address: "localhost:5001"},
	})

	hbResp, err := s.Heartbeat(ctx, &controlpb.HeartbeatRequest{NodeId: "node1"})
	if err != nil {
		t.Fatalf("Heartbeat ni uspel: %v", err)
	}
	if !hbResp.Ok {
		t.Error("Heartbeat bi moral biti OK")
	}

	s.mu.Lock()
	last, ok := s.lastHB["node1"]
	s.mu.Unlock()
	if !ok {
		t.Error("Heartbeat ni zabeležen")
	}
	if time.Since(last) > time.Second {
		t.Error("Čas Heartbeata je prestar")
	}
}

func TestServer_RemoveNode(t *testing.T) {
	s := New(time.Second)
	ctx := context.Background()

	s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node1", Address: "localhost:5001"},
	})
	s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node2", Address: "localhost:5002"},
	})
	s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node3", Address: "localhost:5003"},
	})

	port := s.removeNode("node2")
	if port != "5002" {
		t.Errorf("Pričakovan port 5002, dobljen %s", port)
	}

	chainResp, _ := s.GetChain(ctx, &controlpb.GetChainRequest{})
	if len(chainResp.Chain) != 2 {
		t.Errorf("Pričakovana dolžina verige 2 po odstranitvi, dobljeno %d", len(chainResp.Chain))
	}
	if chainResp.Chain[0].NodeId != "node1" || chainResp.Chain[1].NodeId != "node3" {
		t.Error("Veriga prekinjena po odstranitvi")
	}
}

func TestServer_MonitorFailures(t *testing.T) {
	timeout := 100 * time.Millisecond
	s := New(timeout)
	ctx := context.Background()

	s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node1", Address: "localhost:5001"},
	})

	s.Heartbeat(ctx, &controlpb.HeartbeatRequest{NodeId: "node1"})

	go s.monitorFailures()

	time.Sleep(timeout / 2)
	chainResp, _ := s.GetChain(ctx, &controlpb.GetChainRequest{})
	if len(chainResp.Chain) != 1 {
		t.Error("Vozlišče odstranjeno prezgodaj")
	}

	time.Sleep(600 * time.Millisecond)
	chainResp, _ = s.GetChain(ctx, &controlpb.GetChainRequest{})
	if len(chainResp.Chain) != 0 {
		t.Error("Vozlišče bi moralo biti odstranjeno po poteku časa")
	}
}

func TestServer_GetClusterState(t *testing.T) {
	s := New(time.Second)
	ctx := context.Background()

	s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node1", Address: "localhost:5001"},
	})
	s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node2", Address: "localhost:5002"},
	})

	state, err := s.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetClusterState ni uspel: %v", err)
	}
	if state.Head.NodeId != "node1" {
		t.Error("Napačna glava")
	}
	if state.Tail.NodeId != "node2" {
		t.Error("Napačen rep")
	}
}

func TestRespawner_RegisterAndState(t *testing.T) {
	r := NewRespawner("localhost:9000", "secret")

	r.RegisterNode("node1", "5001")

	state := r.GetNodeState("node1")
	if state == nil {
		t.Fatal("Stanje bi moralo obstajati za registrirano vozlišče")
	}

	r.RegisterNode("node2", "5002")
	r.mu.Lock()
	nodeID, ok := r.portsInUse["5002"]
	r.mu.Unlock()

	if !ok || nodeID != "node2" {
		t.Error("Preslikava porta ni registrirana")
	}
}

func TestRespawner_RequestRespawn(t *testing.T) {
	r := NewRespawner("localhost:9000", "secret")
	r.Start()
	defer r.Stop()

	r.RequestRespawn("node1", "5001")

	time.Sleep(100 * time.Millisecond)

	state := r.GetNodeState("node1")
	if state == nil {
		t.Error("Stanje bi moral ustvariti delavec")
	} else {
		if state.Attempts == 0 {
			t.Error("Število poskusov bi se moralo povečati po obdelavi zahteve")
		}
	}
}

type MockNode struct {
	controlpb.UnimplementedControlPlaneServiceServer
	mu         sync.Mutex
	lastUpdate *controlpb.UpdateNeighborsRequest
	nodeID     string
}

func (m *MockNode) UpdateNeighbors(ctx context.Context, req *controlpb.UpdateNeighborsRequest) (*controlpb.UpdateNeighborsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastUpdate = req
	return &controlpb.UpdateNeighborsResponse{}, nil
}

func startMockNode(t *testing.T, nodeID string) (*MockNode, string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("poslušanje ni uspelo: %v", err)
	}
	s := grpc.NewServer()
	mock := &MockNode{nodeID: nodeID}
	controlpb.RegisterControlPlaneServiceServer(s, mock)
	go func() {
		if err := s.Serve(lis); err != nil {
		}
	}()
	return mock, lis.Addr().String(), func() {
		s.Stop()
		lis.Close()
	}
}

func TestFailover_TailConsistency(t *testing.T) {
	timeout := 500 * time.Millisecond
	s := New(timeout)
	s.respawner = nil

	ctx := context.Background()

	node1, addr1, cleanup1 := startMockNode(t, "node1")
	defer cleanup1()

	_, addr2, cleanup2 := startMockNode(t, "node2")
	defer cleanup2()

	_, err := s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node1", Address: addr1},
	})
	if err != nil {
		t.Fatalf("pridružitev node1 ni uspela: %v", err)
	}

	_, err = s.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "node2", Address: addr2},
	})
	if err != nil {
		t.Fatalf("pridružitev node2 ni uspela: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	clusterState, _ := s.GetClusterState(ctx, &emptypb.Empty{})
	if clusterState.Tail.NodeId != "node2" {
		t.Fatalf("Začetni rep bi moral biti node2, dobljen %s", clusterState.Tail.NodeId)
	}

	node1.mu.Lock()
	if node1.lastUpdate == nil {
		t.Fatal("node1 ni prejel UpdateNeighbors po pridružitvi node2")
	}
	if node1.lastUpdate.Next == nil || node1.lastUpdate.Next.NodeId != "node2" {
		t.Error("node1 bi moral imeti node2 kot naslednika")
	}
	node1.mu.Unlock()

	go s.monitorFailures()

	go func() {
		for i := 0; i < 20; i++ {
			s.Heartbeat(ctx, &controlpb.HeartbeatRequest{NodeId: "node1"})
			time.Sleep(timeout / 3)
		}
	}()

	t.Log("Čakanje na odpoved node2...")
	time.Sleep(2 * time.Second)

	clusterState, _ = s.GetClusterState(ctx, &emptypb.Empty{})
	if clusterState.Tail.NodeId != "node1" {
		t.Errorf("Po preklopu bi moral biti rep node1, dobljen %s", clusterState.Tail.NodeId)
	}
	if len(s.chain) != 1 {
		t.Errorf("Veriga bi morala imeti 1 vozlišče, ima %d", len(s.chain))
	}

	node1.mu.Lock()
	lastUp := node1.lastUpdate
	node1.mu.Unlock()

	if lastUp == nil {
		t.Fatal("node1 ni prejel UpdateNeighbors po preklopu")
	}

	if lastUp.Self.NodeId != "node1" {
		t.Errorf("Cilj posodobitve bi moral biti node1")
	}

	if lastUp.Next != nil {
		t.Errorf("node1 bi moral biti rep (Next=nil), vendar je dobljen Next=%s", lastUp.Next.NodeId)
	}

	if lastUp.Previous != nil {
		t.Errorf("node1 še vedno ne bi smel imeti predhodnika, dobljen %s", lastUp.Previous.NodeId)
	}

	t.Log("Konsistentnost preklopa preverjena: CU trdi, da je node1 rep, Node1 je prejel UpdateNeighbors(Next=nil)")
}
