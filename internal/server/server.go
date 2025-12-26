package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/cluster"
	control "github.com/FedjaMocnik/razpravljalnica/internal/control_unit"
	"github.com/FedjaMocnik/razpravljalnica/internal/replication"
	"github.com/FedjaMocnik/razpravljalnica/internal/storage"
	privatepb "github.com/FedjaMocnik/razpravljalnica/pkgs/private/pb"
	api "github.com/FedjaMocnik/razpravljalnica/pkgs/public/api"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func Zazeni(naslov, nodeID, chainSpec, tokenSecret string) error {
	poslusalec, napaka := net.Listen("tcp", naslov)
	if napaka != nil {
		return fmt.Errorf("ne morem poslušati na %s: %w", naslov, napaka)
	}

	chain, err := cluster.ParseChain(chainSpec)
	if err != nil {
		return err
	}
	self, _, err := chain.Self(nodeID)
	if err != nil {
		return err
	}

	if self.GetAddress() != "" && self.GetAddress() != naslov {
		return fmt.Errorf("naslov (%s) se ne ujema s chain konfiguracijo za %s (%s)", naslov, nodeID, self.GetAddress())
	}

	prev := chain.Prev(nodeID)
	next := chain.Next(nodeID)
	isHead := chain.Head().GetNodeId() == nodeID
	isTail := chain.Tail().GetNodeId() == nodeID

	st := storage.NewState()
	repl := replication.NewManager(replication.Config{
		NodeID:   nodeID,
		Address:  naslov,
		PrevAddr: addrOf(prev),
		NextAddr: addrOf(next),
		IsHead:   isHead,
		IsTail:   isTail,
	}, st)

	grpcStreznik := grpc.NewServer()

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcStreznik, healthSrv)

	razpravljalnicaStreznik := api.NewMessageBoardServer(api.NodeOptions{
		State:       st,
		NodeInfo:    self,
		Chain:       chain.Nodes,
		IsHead:      isHead,
		IsTail:      isTail,
		Replicator:  repl,
		TokenSecret: []byte(tokenSecret),
	})
	pb.RegisterMessageBoardServer(grpcStreznik, razpravljalnicaStreznik)

	controlPlaneStreznik := control.NewControlPlaneServerFromChain(chain)
	pb.RegisterControlPlaneServer(grpcStreznik, controlPlaneStreznik)

	privatepb.RegisterReplicationServiceServer(grpcStreznik, repl)

	reflection.Register(grpcStreznik)

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := repl.BootstrapFromPrev(ctx)
			cancel()
			if err == nil {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	return grpcStreznik.Serve(poslusalec)
}

func addrOf(n *pb.NodeInfo) string {
	if n == nil {
		return ""
	}
	return n.GetAddress()
}
