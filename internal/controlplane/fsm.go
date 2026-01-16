package controlplane

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	"github.com/hashicorp/raft"
)

// CommandType definira tip ukaza za FSM.
type CommandType uint8

const (
	// Operacije nad data-plane verigo.
	CommandJoin CommandType = iota
	CommandRemove
	CommandUpdateNode

	// Operacije nad metapodatki control-plane peerjev.
	CommandUpsertPeer
	CommandRemovePeer
)

// Command je ukaz, ki se aplicira na FSM.
// Za chain operacije: NodeID, Addr.
// Za peer operacije: NodeID, RaftAddr, GRPCAddr.
type Command struct {
	Type     CommandType `json:"type"`
	NodeID   string      `json:"node_id"`
	Addr     string      `json:"addr,omitempty"`
	RaftAddr string      `json:"raft_addr,omitempty"`
	GRPCAddr string      `json:"grpc_addr,omitempty"`
}

// NodeState je stanje posameznega data-plane vozlišča.
type NodeState struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// PeerState je stanje control-plane peerja.
type PeerState struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

// SnapshotState je stanje, ki ga hranimo v snapshotu.
type SnapshotState struct {
	Chain []*NodeState          `json:"chain"`
	Peers map[string]*PeerState `json:"peers"`
}

// FSM implementira raft.FSM: (a) veriga data-plane node-ov, (b) peer metadata control-plane.
type FSM struct {
	mu    sync.RWMutex
	chain []*NodeState
	peers map[string]*PeerState
}

func NewFSM() *FSM {
	return &FSM{chain: make([]*NodeState, 0), peers: make(map[string]*PeerState)}
}

func (f *FSM) Apply(l *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("napaka pri unmarshal: %w", err)
	}

	switch cmd.Type {
	case CommandJoin:
		return f.applyJoin(cmd.NodeID, cmd.Addr)
	case CommandRemove:
		return f.applyRemove(cmd.NodeID)
	case CommandUpdateNode:
		return f.applyUpdateNode(cmd.NodeID, cmd.Addr)
	case CommandUpsertPeer:
		return f.applyUpsertPeer(cmd.NodeID, cmd.RaftAddr, cmd.GRPCAddr)
	case CommandRemovePeer:
		return f.applyRemovePeer(cmd.NodeID)
	default:
		return fmt.Errorf("neznan tip ukaza: %d", cmd.Type)
	}
}

func (f *FSM) applyJoin(nodeID, addr string) any {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i, n := range f.chain {
		if n.NodeID == nodeID {
			f.chain[i].Address = addr
			return nil
		}
	}
	f.chain = append(f.chain, &NodeState{NodeID: nodeID, Address: addr})
	return nil
}

func (f *FSM) applyRemove(nodeID string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, n := range f.chain {
		if n.NodeID == nodeID {
			f.chain = append(f.chain[:i], f.chain[i+1:]...)
			return nil
		}
	}
	return nil
}

func (f *FSM) applyUpdateNode(nodeID, addr string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, n := range f.chain {
		if n.NodeID == nodeID {
			f.chain[i].Address = addr
			return nil
		}
	}
	return nil
}

func (f *FSM) applyUpsertPeer(nodeID, raftAddr, grpcAddr string) any {
	if nodeID == "" {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peers == nil {
		f.peers = make(map[string]*PeerState)
	}
	f.peers[nodeID] = &PeerState{NodeID: nodeID, RaftAddr: raftAddr, GRPCAddr: grpcAddr}
	return nil
}

func (f *FSM) applyRemovePeer(nodeID string) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.peers, nodeID)
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	chain := make([]*NodeState, len(f.chain))
	for i, n := range f.chain {
		chain[i] = &NodeState{NodeID: n.NodeID, Address: n.Address}
	}
	peers := make(map[string]*PeerState, len(f.peers))
	for k, p := range f.peers {
		peers[k] = &PeerState{NodeID: p.NodeID, RaftAddr: p.RaftAddr, GRPCAddr: p.GRPCAddr}
	}

	return &FSMSnapshot{state: SnapshotState{Chain: chain, Peers: peers}}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state SnapshotState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}

	f.mu.Lock()
	f.chain = state.Chain
	if state.Peers == nil {
		state.Peers = make(map[string]*PeerState)
	}
	f.peers = state.Peers
	f.mu.Unlock()
	return nil
}

// ---- Helpers (chain)

func (f *FSM) GetChain() []*controlpb.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	chain := make([]*controlpb.NodeInfo, len(f.chain))
	for i, n := range f.chain {
		chain[i] = &controlpb.NodeInfo{NodeId: n.NodeID, Address: n.Address}
	}
	return chain
}

func (f *FSM) GetHead() *controlpb.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.chain) == 0 {
		return nil
	}
	return &controlpb.NodeInfo{NodeId: f.chain[0].NodeID, Address: f.chain[0].Address}
}

func (f *FSM) GetTail() *controlpb.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.chain) == 0 {
		return nil
	}
	n := f.chain[len(f.chain)-1]
	return &controlpb.NodeInfo{NodeId: n.NodeID, Address: n.Address}
}

func (f *FSM) NodeIndex(nodeID string) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for i, n := range f.chain {
		if n.NodeID == nodeID {
			return i
		}
	}
	return -1
}

// ---- Helpers (peers)

func (f *FSM) GetPeer(nodeID string) (PeerState, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p, ok := f.peers[nodeID]
	if !ok || p == nil {
		return PeerState{}, false
	}
	return *p, true
}

func (f *FSM) ListPeers() []PeerState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]PeerState, 0, len(f.peers))
	for _, p := range f.peers {
		if p == nil {
			continue
		}
		out = append(out, *p)
	}
	return out
}

// FSMSnapshot implementira raft.FSMSnapshot.
type FSMSnapshot struct {
	state SnapshotState
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *FSMSnapshot) Release() {}
