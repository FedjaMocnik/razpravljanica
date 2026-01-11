package controlplane

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSingleNode(t *testing.T) {
	// Ustvari temp dir.
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Konfiguracija za en sam node.
	cfg := RaftConfig{
		NodeID:    "test-node1",
		RaftAddr:  "127.0.0.1:19000",
		GRPCAddr:  "127.0.0.1:19100",
		DataDir:   filepath.Join(tmpDir, "node1"),
		Bootstrap: true,
		HBTimeout: 2 * time.Second,
	}

	// Ustvari server.
	srv, err := NewRaftServer(cfg)
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju raft serverja: %v", err)
	}

	// Zaženi v ozadju.
	go func() {
		if err := srv.Serve(); err != nil {
			t.Logf("server error: %v", err)
		}
	}()

	// Počakaj, da postane leader.
	time.Sleep(3 * time.Second)

	// Preveri, da je leader.
	if !srv.IsLeader() {
		t.Error("pričakoval sem, da bo node leader")
	}

	// Cleanup.
	srv.Shutdown()
}

func TestRaftSingleNodeGRPC(t *testing.T) {
	// Ustvari temp dir.
	tmpDir, err := os.MkdirTemp("", "raft-test-grpc-*")
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Konfiguracija
	cfg := RaftConfig{
		NodeID:    "test-grpc-node",
		RaftAddr:  "127.0.0.1:19001",
		GRPCAddr:  "127.0.0.1:19101",
		DataDir:   filepath.Join(tmpDir, "node1"),
		Bootstrap: true,
		HBTimeout: 2 * time.Second,
	}

	srv, err := NewRaftServer(cfg)
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju raft serverja: %v", err)
	}

	go func() {
		srv.Serve()
	}()

	// Počakaj za zagon
	time.Sleep(3 * time.Second)

	// Poveži se preko gRPC
	conn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("napaka pri povezovanju: %v", err)
	}
	defer conn.Close()

	client := controlpb.NewControlPlaneServiceClient(conn)

	// Test GetRaftState
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := client.GetRaftState(ctx, &controlpb.GetRaftStateRequest{})
	if err != nil {
		t.Fatalf("napaka pri GetRaftState: %v", err)
	}

	if !state.IsLeader {
		t.Error("pričakoval sem, da bo node leader")
	}

	if state.State != "Leader" {
		t.Errorf("pričakoval sem stanje 'Leader', dobil '%s'", state.State)
	}

	t.Logf("Raft stanje: isLeader=%v, state=%s, leaderId=%s", state.IsLeader, state.State, state.LeaderId)

	// Cleanup
	srv.Shutdown()
}

func TestRaftJoinChain(t *testing.T) {
	// Ustvari temp dir
	tmpDir, err := os.MkdirTemp("", "raft-test-chain-*")
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Konfiguracija
	cfg := RaftConfig{
		NodeID:    "test-chain-node",
		RaftAddr:  "127.0.0.1:19002",
		GRPCAddr:  "127.0.0.1:19102",
		DataDir:   filepath.Join(tmpDir, "node1"),
		Bootstrap: true,
		HBTimeout: 2 * time.Second,
	}

	srv, err := NewRaftServer(cfg)
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju raft serverja: %v", err)
	}

	go func() {
		srv.Serve()
	}()

	time.Sleep(3 * time.Second)

	// Poveži se preko gRPC
	conn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("napaka pri povezovanju: %v", err)
	}
	defer conn.Close()

	client := controlpb.NewControlPlaneServiceClient(conn)
	ctx := context.Background()

	// Test JoinChain
	joinResp, err := client.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{
			NodeId:  "server1",
			Address: "localhost:8001",
		},
	})
	if err != nil {
		t.Fatalf("napaka pri JoinChain: %v", err)
	}

	t.Logf("JoinChain response: prev=%v, next=%v", joinResp.Previous, joinResp.Next)

	// Dodaj še en node
	joinResp2, err := client.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{
			NodeId:  "server2",
			Address: "localhost:8002",
		},
	})
	if err != nil {
		t.Fatalf("napaka pri JoinChain (2): %v", err)
	}

	// server2 mora imeti server1 kot previous
	if joinResp2.Previous == nil || joinResp2.Previous.NodeId != "server1" {
		t.Errorf("pričakoval sem previous=server1, dobil %v", joinResp2.Previous)
	}

	// Test GetChain
	chainResp, err := client.GetChain(ctx, &controlpb.GetChainRequest{})
	if err != nil {
		t.Fatalf("napaka pri GetChain: %v", err)
	}

	if len(chainResp.Chain) != 2 {
		t.Errorf("pričakoval sem 2 node-a v verigi, dobil %d", len(chainResp.Chain))
	}

	if chainResp.Head == nil || chainResp.Head.NodeId != "server1" {
		t.Errorf("pričakoval sem head=server1, dobil %v", chainResp.Head)
	}

	if chainResp.Tail == nil || chainResp.Tail.NodeId != "server2" {
		t.Errorf("pričakoval sem tail=server2, dobil %v", chainResp.Tail)
	}

	t.Logf("Chain: head=%s, tail=%s, len=%d", chainResp.Head.NodeId, chainResp.Tail.NodeId, len(chainResp.Chain))

	srv.Shutdown()
}

func TestRaftThreeNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("preskakujem dolg test v short mode")
	}

	// Ustvari temp dir
	tmpDir, err := os.MkdirTemp("", "raft-test-3nodes-*")
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Konfiguracija za 3 node-e
	configs := []RaftConfig{
		{
			NodeID:    "node1",
			RaftAddr:  "127.0.0.1:19010",
			GRPCAddr:  "127.0.0.1:19110",
			DataDir:   filepath.Join(tmpDir, "node1"),
			Bootstrap: true,
			HBTimeout: 2 * time.Second,
		},
		{
			NodeID:    "node2",
			RaftAddr:  "127.0.0.1:19011",
			GRPCAddr:  "127.0.0.1:19111",
			DataDir:   filepath.Join(tmpDir, "node2"),
			HBTimeout: 2 * time.Second,
		},
		{
			NodeID:    "node3",
			RaftAddr:  "127.0.0.1:19012",
			GRPCAddr:  "127.0.0.1:19112",
			DataDir:   filepath.Join(tmpDir, "node3"),
			HBTimeout: 2 * time.Second,
		},
	}

	servers := make([]*RaftServer, 3)

	// Zaženi prvi node (bootstrap)
	srv1, err := NewRaftServer(configs[0])
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju srv1: %v", err)
	}
	servers[0] = srv1

	go func() {
		srv1.Serve()
	}()

	// Počakaj, da postane leader
	time.Sleep(3 * time.Second)

	if !srv1.IsLeader() {
		t.Fatal("srv1 bi moral biti leader")
	}

	// Poveži na leader in dodaj node2
	conn1, err := grpc.NewClient(configs[0].GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("napaka pri povezovanju na srv1: %v", err)
	}
	defer conn1.Close()

	client1 := controlpb.NewControlPlaneServiceClient(conn1)
	ctx := context.Background()

	// Dodaj node2 v Raft cluster
	addResp, err := client1.AddPeer(ctx, &controlpb.AddPeerRequest{
		NodeId:   "node2",
		RaftAddr: configs[1].RaftAddr,
		GrpcAddr: configs[1].GRPCAddr,
	})
	if err != nil {
		t.Fatalf("napaka pri AddPeer (node2): %v", err)
	}
	if !addResp.Ok {
		t.Fatalf("AddPeer (node2) ni uspel: %s", addResp.Error)
	}

	// Zaženi node2
	srv2, err := NewRaftServer(configs[1])
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju srv2: %v", err)
	}
	servers[1] = srv2

	go func() {
		srv2.Serve()
	}()

	time.Sleep(2 * time.Second)

	// Dodaj node3 v Raft cluster
	addResp3, err := client1.AddPeer(ctx, &controlpb.AddPeerRequest{
		NodeId:   "node3",
		RaftAddr: configs[2].RaftAddr,
		GrpcAddr: configs[2].GRPCAddr,
	})
	if err != nil {
		t.Fatalf("napaka pri AddPeer (node3): %v", err)
	}
	if !addResp3.Ok {
		t.Fatalf("AddPeer (node3) ni uspel: %s", addResp3.Error)
	}

	// Zaženi node3
	srv3, err := NewRaftServer(configs[2])
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju srv3: %v", err)
	}
	servers[2] = srv3

	go func() {
		srv3.Serve()
	}()

	time.Sleep(2 * time.Second)

	// Preveri stanje clusterja
	state, err := client1.GetRaftState(ctx, &controlpb.GetRaftStateRequest{})
	if err != nil {
		t.Fatalf("napaka pri GetRaftState: %v", err)
	}

	t.Logf("Cluster state: isLeader=%v, leaderId=%s, peers=%d", state.IsLeader, state.LeaderId, len(state.Peers))

	// Dodaj server nodes v chain in preveri replikacijo
	_, err = client1.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "data-server1", Address: "localhost:7001"},
	})
	if err != nil {
		t.Fatalf("napaka pri JoinChain: %v", err)
	}

	// Počakaj za replikacijo
	time.Sleep(1 * time.Second)

	// Preveri, da vsi node-i vidijo isto verigo
	t.Log("PReverjamo replikacijo.")
	allOk := true
	for i, srv := range servers {
		chain := srv.fsm.GetChain()
		if len(chain) != 1 {
			t.Errorf("node%d: pričakoval sem 1 node v verigi, dobil %d", i+1, len(chain))
			allOk = false
		} else if chain[0].GetNodeId() != "data-server1" {
			t.Errorf("node%d: pričakoval sem data-server1, dobil %s", i+1, chain[0].GetNodeId())
			allOk = false
		} else {
			t.Logf("node%d: chain=[%s] - REPLICIRANO PRAVILNO", i+1, chain[0].GetNodeId())
		}
	}

	if allOk {
		t.Log("Vsi 3 replicirajo pravilno.")
	} else {
		t.Error("Replikacija ni uspela.")
	}

	// Cleanup
	for _, srv := range servers {
		if srv != nil {
			srv.Shutdown()
		}
	}
}

func TestFSM(t *testing.T) {
	fsm := NewFSM()

	// Test Join
	fsm.applyJoin("node1", "localhost:8001")
	fsm.applyJoin("node2", "localhost:8002")
	fsm.applyJoin("node3", "localhost:8003")

	chain := fsm.GetChain()
	if len(chain) != 3 {
		t.Errorf("pričakoval sem 3 node-e, dobil %d", len(chain))
	}

	// Test Head/Tail
	head := fsm.GetHead()
	if head == nil || head.NodeId != "node1" {
		t.Errorf("pričakoval sem head=node1, dobil %v", head)
	}

	tail := fsm.GetTail()
	if tail == nil || tail.NodeId != "node3" {
		t.Errorf("pričakoval sem tail=node3, dobil %v", tail)
	}

	// Test Remove
	fsm.applyRemove("node2")
	chain = fsm.GetChain()
	if len(chain) != 2 {
		t.Errorf("pričakoval sem 2 node-a po remove, dobil %d", len(chain))
	}

	// node1 -> node3
	if chain[0].NodeId != "node1" || chain[1].NodeId != "node3" {
		t.Errorf("pričakoval sem [node1, node3], dobil [%s, %s]", chain[0].NodeId, chain[1].NodeId)
	}

	// Test Update
	fsm.applyUpdateNode("node1", "localhost:9001")
	chain = fsm.GetChain()
	if chain[0].Address != "localhost:9001" {
		t.Errorf("pričakoval sem naslov localhost:9001, dobil %s", chain[0].Address)
	}

	// Test NodeIndex
	idx := fsm.NodeIndex("node3")
	if idx != 1 {
		t.Errorf("pričakoval sem index 1 za node3, dobil %d", idx)
	}

	idx = fsm.NodeIndex("nonexistent")
	if idx != -1 {
		t.Errorf("pričakoval sem index -1 za nonexistent, dobil %d", idx)
	}
}

func TestRaftGetClusterState(t *testing.T) {
	// Ustvari temp dir
	tmpDir, err := os.MkdirTemp("", "raft-test-cluster-state-*")
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := RaftConfig{
		NodeID:    "cluster-state-node",
		RaftAddr:  "127.0.0.1:19003",
		GRPCAddr:  "127.0.0.1:19103",
		DataDir:   filepath.Join(tmpDir, "node1"),
		Bootstrap: true,
		HBTimeout: 2 * time.Second,
	}

	srv, err := NewRaftServer(cfg)
	if err != nil {
		t.Fatalf("napaka pri ustvarjanju raft serverja: %v", err)
	}

	go func() {
		srv.Serve()
	}()

	time.Sleep(3 * time.Second)

	conn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("napaka pri povezovanju: %v", err)
	}
	defer conn.Close()

	// Uporabi public API
	publicClient := controlpb.NewControlPlaneServiceClient(conn)
	ctx := context.Background()

	// Najprej dodaj node-e v chain
	publicClient.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "head-node", Address: "localhost:5001"},
	})
	publicClient.JoinChain(ctx, &controlpb.JoinChainRequest{
		Node: &controlpb.NodeInfo{NodeId: "tail-node", Address: "localhost:5002"},
	})

	// Pridobi cluster state preko public API
	// Uporabimo direktno srv ker nimamo public client tukaj
	clusterState, err := srv.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("napaka pri GetClusterState: %v", err)
	}

	if clusterState.Head == nil || clusterState.Head.NodeId != "head-node" {
		t.Errorf("pričakoval sem head=head-node, dobil %v", clusterState.Head)
	}

	if clusterState.Tail == nil || clusterState.Tail.NodeId != "tail-node" {
		t.Errorf("pričakoval sem tail=tail-node, dobil %v", clusterState.Tail)
	}

	t.Logf("ClusterState: head=%s, tail=%s", clusterState.Head.NodeId, clusterState.Tail.NodeId)

	srv.Shutdown()
}
