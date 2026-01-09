package server

import (
	"context"
	"log"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/replication"
	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	api "github.com/FedjaMocnik/razpravljalnica/pkgs/public/api"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func joinAndHeartbeat(controlAddr, nodeID, nodeAddr string, repl *replication.Manager, mb *api.MessageBoardServer) {
	for {
		cc, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("node %s: control dial failed: %v", nodeID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		cli := controlpb.NewControlPlaneServiceClient(cc)

		// Join
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := cli.JoinChain(ctx, &controlpb.JoinChainRequest{Node: &controlpb.NodeInfo{NodeId: nodeID, Address: nodeAddr}})
			cancel()
			if err == nil {
				applyJoinResponse(cli, nodeID, repl, mb, resp)
				break
			}
			log.Printf("node %s: JoinChain failed: %v", nodeID, err)
			time.Sleep(500 * time.Millisecond)
		}

		// Heartbeat loop
		ticker := time.NewTicker(500 * time.Millisecond)
		ok := true
		for ok {
			<-ticker.C
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			_, err := cli.Heartbeat(ctx, &controlpb.HeartbeatRequest{NodeId: nodeID})
			cancel()
			if err != nil {
				log.Printf("node %s: heartbeat failed: %v", nodeID, err)
				ok = false
			}
		}
		ticker.Stop()
		_ = cc.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func applyJoinResponse(cli controlpb.ControlPlaneServiceClient, nodeID string, repl *replication.Manager, mb *api.MessageBoardServer, resp *controlpb.JoinChainResponse) {
	if resp == nil {
		return
	}
	prevAddr := ""
	if resp.GetPrevious() != nil {
		prevAddr = resp.GetPrevious().GetAddress()
	}
	nextAddr := ""
	if resp.GetNext() != nil {
		nextAddr = resp.GetNext().GetAddress()
	}
	isHead := prevAddr == ""
	isTail := nextAddr == ""
	prevChanged := false
	if repl != nil {
		prevChanged = repl.UpdateTopology(prevAddr, nextAddr, isHead, isTail)
	}
	// chain fetch
	chain := fetchChain(cli)
	if mb != nil {
		mb.UpdateClusterConfig(chain, isHead, isTail)
	}
	// initial sync
	if prevChanged && prevAddr != "" && repl != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = repl.ResyncFromPrev(ctx)
		}()
	}
}

func fetchChain(cli controlpb.ControlPlaneServiceClient) []*publicpb.NodeInfo {
	if cli == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	resp, err := cli.GetChain(ctx, &controlpb.GetChainRequest{})
	if err != nil {
		return nil
	}
	out := make([]*publicpb.NodeInfo, 0, len(resp.GetChain()))
	for _, n := range resp.GetChain() {
		if n == nil {
			continue
		}
		out = append(out, &publicpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
	}
	return out
}
