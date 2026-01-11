package nodeadmin

import (
	"context"
	"log"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/controlclient"
	"github.com/FedjaMocnik/razpravljalnica/internal/replication"
	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	api "github.com/FedjaMocnik/razpravljalnica/pkgs/public/api"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
)

// ControlNodeServer implementira ControlPlaneService na strani node-a
// samo zato, da lahko control unit kliče UpdateNeighbors.
type ControlNodeServer struct {
	controlpb.UnimplementedControlPlaneServiceServer

	nodeID  string
	control *controlclient.Client

	repl *replication.Manager
	mb   *api.MessageBoardServer
}

func New(nodeID string, control *controlclient.Client, repl *replication.Manager, mb *api.MessageBoardServer) *ControlNodeServer {
	return &ControlNodeServer{nodeID: nodeID, control: control, repl: repl, mb: mb}
}

func (s *ControlNodeServer) UpdateNeighbors(ctx context.Context, req *controlpb.UpdateNeighborsRequest) (*controlpb.UpdateNeighborsResponse, error) {
	// Posodobimo sosede (prev/next) v repl managerju in v public API strežniku.
	prevAddr := ""
	if req.GetPrevious() != nil {
		prevAddr = req.GetPrevious().GetAddress()
	}
	nextAddr := ""
	if req.GetNext() != nil {
		nextAddr = req.GetNext().GetAddress()
	}
	isHead := prevAddr == ""
	isTail := nextAddr == ""

	prevChanged := false
	if s.repl != nil {
		prevChanged = s.repl.UpdateTopology(prevAddr, nextAddr, isHead, isTail)
	}

	// Osveži celoten chain (za hashing naročnin).
	chain := s.fetchChain()
	if s.mb != nil {
		s.mb.UpdateClusterConfig(chain, isHead, isTail)
	}

	// Če se je previous spremenil, izvedemo catch-up (UpdateRequest) proti novemu previous.
	if prevChanged && prevAddr != "" && s.repl != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.repl.ResyncFromPrev(ctx); err != nil {
				log.Printf("node %s: resync failed: %v", s.nodeID, err)
			}
		}()
	}

	return &controlpb.UpdateNeighborsResponse{Ok: true}, nil
}

func (s *ControlNodeServer) fetchChain() []*publicpb.NodeInfo {
	if s.control == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	chain, err := s.control.GetChain(ctx)
	if err != nil {
		return nil
	}
	return chain
}
