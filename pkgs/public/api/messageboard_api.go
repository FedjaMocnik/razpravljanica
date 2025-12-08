package api

import (
	"context"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MessageBoardServer struct {
	pb.UnimplementedMessageBoardServer
}

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {

	return nil, nil
}
