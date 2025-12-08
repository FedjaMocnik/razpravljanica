package public_api

import (
	"context"
	"time"

	public_pb "github.com/FedjaMocnik/razpravljalnica/public_pb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MessageBoardServer struct {
	public_pb.UnimplementedMessageBoardServer
}
