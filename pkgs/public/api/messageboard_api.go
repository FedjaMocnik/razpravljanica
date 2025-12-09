package api

import (
	"context"
	// "time"

	"github.com/FedjaMocnik/razpravljalnica/internal/storage"
	"github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	// "google.golang.org/protobuf/types/known/emptypb"
	// "google.golang.org/protobuf/types/known/timestamppb"
)

type MessageBoardServer struct {
	pb.UnimplementedMessageBoardServer

	server_state *storage.State
}

func NewMessageBoardServer() *MessageBoardServer {
	return &MessageBoardServer{
		server_state: storage.NewState(),
	}
}

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	user := &pb.User{
		Id:   s.server_state.GetUserID(),
		Name: req.GetName(),
	}

	s.server_state.AddUser(user)

	return user, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	topic := &pb.Topic{
		Id:   s.server_state.GetTopicID(),
		Name: req.GetName(),
	}

	s.server_state.AddTopic(topic)

	return topic, nil
}
