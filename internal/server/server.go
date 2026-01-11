package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/cluster"
	control "github.com/FedjaMocnik/razpravljalnica/internal/control_unit"
	"github.com/FedjaMocnik/razpravljalnica/internal/controlclient"
	"github.com/FedjaMocnik/razpravljalnica/internal/nodeadmin"
	"github.com/FedjaMocnik/razpravljalnica/internal/replication"
	"github.com/FedjaMocnik/razpravljalnica/internal/storage"
	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	privatepb "github.com/FedjaMocnik/razpravljalnica/pkgs/private/pb"
	api "github.com/FedjaMocnik/razpravljalnica/pkgs/public/api"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func Zazeni(naslov, nodeID, chainSpec, tokenSecret, controlAddr string) error {
	poslusalec, napaka := net.Listen("tcp", naslov)
	if napaka != nil {
		return fmt.Errorf("ne morem poslušati na %s: %w", naslov, napaka)
	}

	// Statik: chainSpec (1. del naloge). Dinamičen: control unit.
	var (
		chain  *cluster.ChainConfig
		self   *pb.NodeInfo
		prev   *pb.NodeInfo
		next   *pb.NodeInfo
		isHead bool
		isTail bool
	)
	var err error

	if controlAddr == "" {
		chain, err = cluster.ParseChain(chainSpec)
		if err != nil {
			return err
		}
		self, _, err = chain.Self(nodeID)
		if err != nil {
			return err
		}

		// Če imamo naslov in ni enak željenemu -> err.
		if self.GetAddress() != "" && self.GetAddress() != naslov {
			return fmt.Errorf("naslov (%s) se ne ujema s chain konfiguracijo za %s (%s)", naslov, nodeID, self.GetAddress())
		}

		prev = chain.Prev(nodeID)
		next = chain.Next(nodeID)
		isHead = chain.Head().GetNodeId() == nodeID
		isTail = chain.Tail().GetNodeId() == nodeID
	} else {
		// Minimalna začetna konfiguracija; control unit jo bo kasneje poslal.
		self = &pb.NodeInfo{NodeId: nodeID, Address: naslov}
		chain = &cluster.ChainConfig{Nodes: []*pb.NodeInfo{self}}
		prev, next = nil, nil
		isHead, isTail = false, false
	}

	var ctrlClient *controlclient.Client
	if controlAddr != "" {
		ctrlClient = controlclient.New(controlclient.ParseAddrs(controlAddr))
	}

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

	// Node-side control service (UpdateNeighbors) – uporablja ga dedicated control unit.
	controlpb.RegisterControlPlaneServiceServer(grpcStreznik, nodeadmin.New(nodeID, ctrlClient, repl, razpravljalnicaStreznik))

	// V statični postavitvi (1. del) ima vsak node tudi public ControlPlane (GetClusterState).
	if controlAddr == "" {
		controlPlaneStreznik := control.NewControlPlaneServerFromChain(chain)
		pb.RegisterControlPlaneServer(grpcStreznik, controlPlaneStreznik)
	}

	privatepb.RegisterReplicationServiceServer(grpcStreznik, repl)

	reflection.Register(grpcStreznik)

	// gRPC serve v gorutini, da se lahko v dinamičnem režimu najprej priključimo control unit-u.
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcStreznik.Serve(poslusalec) }()

	// Bootstrap followerjev iz prev (deluje tudi v statiki; v dinamičnem režimu bo prev nastavljen po Join/UpdateNeighbors).
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

	if ctrlClient != nil {
		go joinAndHeartbeat(ctrlClient, nodeID, naslov, repl, razpravljalnicaStreznik)
	}

	return <-serveErr
}

func addrOf(n *pb.NodeInfo) string {
	if n == nil {
		return ""
	}
	return n.GetAddress()
}
