package server

import (
	"fmt"
	"net"

	control "github.com/FedjaMocnik/razpravljalnica/internal/control_unit"
	api "github.com/FedjaMocnik/razpravljalnica/pkgs/public/api"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"

	"google.golang.org/grpc"
)

func Zazeni(naslov string) error {
	poslusalec, napaka := net.Listen("tcp", naslov)
	if napaka != nil {
		return fmt.Errorf("ne morem poslušati na %s: %w", naslov, napaka)
	}

	grpcStreznik := grpc.NewServer()

	razpravljalnicaStreznik := api.NewMessageBoardServerZNaslovom(naslov)
	pb.RegisterMessageBoardServer(grpcStreznik, razpravljalnicaStreznik)

	controlPlaneStreznik := control.NewControlPlaneServer(naslov)
	pb.RegisterControlPlaneServer(grpcStreznik, controlPlaneStreznik)

	return grpcStreznik.Serve(poslusalec)
}
