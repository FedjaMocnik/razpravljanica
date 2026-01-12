package server

import (
	"context"
	"log"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/controlclient"
	"github.com/FedjaMocnik/razpravljalnica/internal/replication"
	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	api "github.com/FedjaMocnik/razpravljalnica/pkgs/public/api"
)

func joinAndHeartbeat(ctrl *controlclient.Client, nodeID, nodeAddr string, repl *replication.Manager, mb *api.MessageBoardServer) {
	for {
		// Join (ponavljaj do uspeha).
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			resp, err := ctrl.JoinChain(ctx, nodeID, nodeAddr)
			cancel()
			if err == nil {
				applyJoinResponse(ctrl, nodeID, repl, mb, resp)
				break
			}
			log.Printf("node %s: JoinChain ni uspel: %v", nodeID, err)
			time.Sleep(500 * time.Millisecond)
		}

		// Heartbeat loop.
		ticker := time.NewTicker(500 * time.Millisecond)
		ok := true
		for ok {
			<-ticker.C
			ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
			_, err := ctrl.Heartbeat(ctx, nodeID)
			cancel()
			if err != nil {
				log.Printf("node %s: ni uspel poslati heartbeat: %v", nodeID, err)
				ok = false
			}
		}
		ticker.Stop()
		time.Sleep(500 * time.Millisecond)
	}
}

func applyJoinResponse(ctrl *controlclient.Client, nodeID string, repl *replication.Manager, mb *api.MessageBoardServer, resp *controlpb.JoinChainResponse) {
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
	chain, _ := ctrl.GetChain(context.Background())
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
