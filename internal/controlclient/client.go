package controlclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client je odjemalec za ControlPlaneService, ki podpira failover.
// Uporablja seznam naslovov (seed-e) in ob napaki preklopi na naslednjega.
// Če server vrne leader_addr (HeartbeatResponse), Client preklopi na leaderja.
type Client struct {
	mu sync.Mutex

	addrs []string
	idx   int

	conns map[string]*grpc.ClientConn
	clis  map[string]controlpb.ControlPlaneServiceClient
}

// ParseAddrs sprejme "a,b,c" ali " a , b " in vrne normaliziran seznam.
func ParseAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		out = append(out, a)
	}
	return out
}

func New(addrs []string) *Client {
	// Dedupe, ohrani vrstni red.
	seen := map[string]bool{}
	uniq := make([]string, 0, len(addrs))
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		uniq = append(uniq, a)
	}
	return &Client{
		addrs: uniq,
		idx:   0,
		conns: make(map[string]*grpc.ClientConn),
		clis:  make(map[string]controlpb.ControlPlaneServiceClient),
	}
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		_ = cc.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
	c.clis = make(map[string]controlpb.ControlPlaneServiceClient)
}

func (c *Client) dial(addr string) (controlpb.ControlPlaneServiceClient, error) {
	c.mu.Lock()
	if cli := c.clis[addr]; cli != nil {
		c.mu.Unlock()
		return cli, nil
	}
	c.mu.Unlock()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	cli := controlpb.NewControlPlaneServiceClient(cc)

	c.mu.Lock()
	// V primeru race: raje uporabi že shranjenega.
	if existing := c.clis[addr]; existing != nil {
		c.mu.Unlock()
		_ = cc.Close()
		return existing, nil
	}
	c.conns[addr] = cc
	c.clis[addr] = cli
	c.mu.Unlock()
	return cli, nil
}

func (c *Client) currentAddrsLocked() []string {
	if len(c.addrs) == 0 {
		return nil
	}
	// Vrnemo seznam za poskus: trenutni indeks, nato ostali.
	out := make([]string, 0, len(c.addrs))
	for i := 0; i < len(c.addrs); i++ {
		out = append(out, c.addrs[(c.idx+i)%len(c.addrs)])
	}
	return out
}

func (c *Client) setPreferred(addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Če je že v seznamu, premakni idx.
	for i, a := range c.addrs {
		if a == addr {
			c.idx = i
			return
		}
	}
	// Sicer ga dodaj na konec in nastavi idx nanj.
	c.addrs = append(c.addrs, addr)
	c.idx = len(c.addrs) - 1
}

var ErrNoControlAddrs = errors.New("ni podan noben control plane naslov")

// call poskusi z več naslovi; ob uspehu vrne odgovor.
func (c *Client) call(ctx context.Context, fn func(controlpb.ControlPlaneServiceClient) error) error {
	c.mu.Lock()
	addrs := c.currentAddrsLocked()
	c.mu.Unlock()
	if len(addrs) == 0 {
		return ErrNoControlAddrs
	}

	var lastErr error
	for _, addr := range addrs {
		cli, err := c.dial(addr)
		if err != nil {
			lastErr = err
			continue
		}
		if err := fn(cli); err != nil {
			lastErr = err
			continue
		}
		// Uspeh -> ta addr postane preferiran.
		c.setPreferred(addr)
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("control call failed")
	}
	return lastErr
}

func (c *Client) JoinChain(ctx context.Context, nodeID, nodeAddr string) (*controlpb.JoinChainResponse, error) {
	var out *controlpb.JoinChainResponse
	err := c.call(ctx, func(cli controlpb.ControlPlaneServiceClient) error {
		// Join je lahko počasnejši, zato lokalno povečamo timeout, če ga caller ni nastavil.
		lctx := ctx
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
		}
		resp, err := cli.JoinChain(lctx, &controlpb.JoinChainRequest{Node: &controlpb.NodeInfo{NodeId: nodeID, Address: nodeAddr}})
		if err != nil {
			return err
		}
		out = resp
		return nil
	})
	return out, err
}

func (c *Client) Heartbeat(ctx context.Context, nodeID string) (*controlpb.HeartbeatResponse, error) {
	var out *controlpb.HeartbeatResponse
	err := c.call(ctx, func(cli controlpb.ControlPlaneServiceClient) error {
		lctx := ctx
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, 900*time.Millisecond)
			defer cancel()
		}
		resp, err := cli.Heartbeat(lctx, &controlpb.HeartbeatRequest{NodeId: nodeID})
		if err != nil {
			return err
		}
		out = resp
		if resp != nil && resp.GetLeaderAddr() != "" {
			c.setPreferred(resp.GetLeaderAddr())
		}
		return nil
	})
	return out, err
}

func (c *Client) GetChain(ctx context.Context) ([]*publicpb.NodeInfo, error) {
	var out []*publicpb.NodeInfo
	err := c.call(ctx, func(cli controlpb.ControlPlaneServiceClient) error {
		lctx := ctx
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, 900*time.Millisecond)
			defer cancel()
		}
		resp, err := cli.GetChain(lctx, &controlpb.GetChainRequest{})
		if err != nil {
			return err
		}
		out = out[:0]
		for _, n := range resp.GetChain() {
			if n == nil {
				continue
			}
			out = append(out, &publicpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
		}
		return nil
	})
	return out, err
}
