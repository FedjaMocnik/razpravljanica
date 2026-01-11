package main

import (
	"context"
	"fmt"
	"os"
	"time"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run tools/raftstate.go host:port")
		os.Exit(2)
	}
	addr := os.Args[1]

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	cli := controlpb.NewControlPlaneServiceClient(conn)
	st, err := cli.GetRaftState(ctx, &controlpb.GetRaftStateRequest{})
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s -> state=%s isLeader=%v leaderId=%s leaderAddr=%s peers=%d\n",
		addr, st.GetState(), st.GetIsLeader(), st.GetLeaderId(), st.GetLeaderAddr(), len(st.GetPeers()))

	for _, p := range st.GetPeers() {
		fmt.Printf("  - %s raft=%s grpc=%s\n", p.GetNodeId(), p.GetRaftAddr(), p.GetGrpcAddr())
	}
}
