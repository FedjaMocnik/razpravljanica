package controlplane

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	"github.com/hashicorp/raft"
)

// CommandType definira tip ukaza za FSM
type CommandType uint8

const (
	CommandJoin CommandType = iota
	CommandRemove
	CommandUpdateNode
)

// Command je ukaz, ki se aplicira na FSM
type Command struct {
	Type   CommandType `json:"type"`
	NodeID string      `json:"node_id"`
	Addr   string      `json:"addr"`
}

// ChainState je stanje verige (chain), ki jo hranimo v FSM
type ChainState struct {
	Chain []*NodeState `json:"chain"`
}

// NodeState je stanje posameznega vozlišča
type NodeState struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// FSM implementira raft.FSM za hranjenje stanja verige
type FSM struct {
	mu    sync.RWMutex
	chain []*NodeState
}

// NewFSM ustvari novo FSM instanco
func NewFSM() *FSM {
	return &FSM{
		chain: make([]*NodeState, 0),
	}
}

// Apply aplicira raft log vnos na FSM
func (f *FSM) Apply(l *raft.Log) interface{} {
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
		return f.applyUpdate(cmd.NodeID, cmd.Addr)
	default:
		return fmt.Errorf("neznan tip ukaza: %d", cmd.Type)
	}
}

func (f *FSM) applyJoin(nodeID, addr string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Preveri če že obstaja
	for i, n := range f.chain {
		if n.NodeID == nodeID {
			// Posodobi naslov
			f.chain[i].Address = addr
			return nil
		}
	}

	// Dodaj na konec verige
	f.chain = append(f.chain, &NodeState{
		NodeID:  nodeID,
		Address: addr,
	})
	return nil
}

func (f *FSM) applyRemove(nodeID string) interface{} {
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

func (f *FSM) applyUpdate(nodeID, addr string) interface{} {
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

// Snapshot vrne FSMSnapshot za persistenco
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Kopiramo stanje
	chain := make([]*NodeState, len(f.chain))
	for i, n := range f.chain {
		chain[i] = &NodeState{
			NodeID:  n.NodeID,
			Address: n.Address,
		}
	}

	return &FSMSnapshot{state: ChainState{Chain: chain}}, nil
}

// Restore obnovi stanje iz snapshot-a
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state ChainState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}

	f.mu.Lock()
	f.chain = state.Chain
	f.mu.Unlock()

	return nil
}

// GetChain vrne kopijo trenutne verige
func (f *FSM) GetChain() []*controlpb.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	chain := make([]*controlpb.NodeInfo, len(f.chain))
	for i, n := range f.chain {
		chain[i] = &controlpb.NodeInfo{
			NodeId:  n.NodeID,
			Address: n.Address,
		}
	}
	return chain
}

// GetHead vrne glavo verige
func (f *FSM) GetHead() *controlpb.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if len(f.chain) == 0 {
		return nil
	}
	return &controlpb.NodeInfo{
		NodeId:  f.chain[0].NodeID,
		Address: f.chain[0].Address,
	}
}

// GetTail vrne rep verige
func (f *FSM) GetTail() *controlpb.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if len(f.chain) == 0 {
		return nil
	}
	n := f.chain[len(f.chain)-1]
	return &controlpb.NodeInfo{
		NodeId:  n.NodeID,
		Address: n.Address,
	}
}

// NodeIndex vrne indeks vozlišča v verigi ali -1
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

// FSMSnapshot implementira raft.FSMSnapshot
type FSMSnapshot struct {
	state ChainState
}

// Persist shrani snapshot
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

// Release sprosti vire
func (s *FSMSnapshot) Release() {}
