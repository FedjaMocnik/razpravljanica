package cluster

import (
	"fmt"
	"strings"

	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
)

type ChainConfig struct {
	Nodes []*publicpb.NodeInfo
}

func ParseChain(spec string) (*ChainConfig, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, fmt.Errorf("prazna veriga (chain)")
	}

	parts := strings.Split(spec, ",")
	nodes := make([]*publicpb.NodeInfo, 0, len(parts))

	seenID := map[string]struct{}{}
	seenAddr := map[string]struct{}{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("neveljaven chain element %q (pričakujem nodeId=host:port)", p)
		}
		id := strings.TrimSpace(kv[0])
		addr := strings.TrimSpace(kv[1])
		if id == "" || addr == "" {
			return nil, fmt.Errorf("neveljaven chain element %q (prazen id ali naslov)", p)
		}
		if _, ok := seenID[id]; ok {
			return nil, fmt.Errorf("podvojen node_id v chain: %s", id)
		}
		if _, ok := seenAddr[addr]; ok {
			return nil, fmt.Errorf("podvojen naslov v chain: %s", addr)
		}
		seenID[id] = struct{}{}
		seenAddr[addr] = struct{}{}
		nodes = append(nodes, &publicpb.NodeInfo{NodeId: id, Address: addr})
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("prazna veriga (chain)")
	}
	return &ChainConfig{Nodes: nodes}, nil
}

func (c *ChainConfig) Head() *publicpb.NodeInfo {
	if c == nil || len(c.Nodes) == 0 {
		return nil
	}
	return c.Nodes[0]
}

func (c *ChainConfig) Tail() *publicpb.NodeInfo {
	if c == nil || len(c.Nodes) == 0 {
		return nil
	}
	return c.Nodes[len(c.Nodes)-1]
}

func (c *ChainConfig) IndexOf(nodeID string) int {
	for i, n := range c.Nodes {
		if n.NodeId == nodeID {
			return i
		}
	}
	return -1
}

func (c *ChainConfig) Self(nodeID string) (*publicpb.NodeInfo, int, error) {
	idx := c.IndexOf(nodeID)
	if idx < 0 {
		return nil, -1, fmt.Errorf("node_id %q ni v chain", nodeID)
	}
	return c.Nodes[idx], idx, nil
}

func (c *ChainConfig) Prev(nodeID string) *publicpb.NodeInfo {
	idx := c.IndexOf(nodeID)
	if idx <= 0 {
		return nil
	}
	return c.Nodes[idx-1]
}

func (c *ChainConfig) Next(nodeID string) *publicpb.NodeInfo {
	idx := c.IndexOf(nodeID)
	if idx < 0 || idx+1 >= len(c.Nodes) {
		return nil
	}
	return c.Nodes[idx+1]
}
