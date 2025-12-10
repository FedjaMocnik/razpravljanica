package api

import (
	"context"
	// "fmt"

	storage "github.com/FedjaMocnik/razpravljalnica/internal/storage"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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
	// fmt.Printf("CreateUser: ime=%s\n", req.GetName())

	user := &pb.User{
		Id:   s.server_state.GetUserID(),
		Name: req.GetName(),
	}

	s.server_state.AddUser(user)

	// fmt.Printf("CreateUser: ustvarjen uporabnik id=%d, ime=%s\n", user.Id, user.Name)
	return user, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	// fmt.Printf("CreateTopic: ime=%s\n", req.GetName())

	topic := &pb.Topic{
		Id:   s.server_state.GetTopicID(),
		Name: req.GetName(),
	}

	s.server_state.AddTopic(topic)

	// fmt.Printf("CreateTopic: ustvarjena tema id=%d, ime=%s\n", topic.Id, topic.Name)
	return topic, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	// fmt.Printf("PostMessage: tema_id=%d, uporabnik_id=%d, besedilo=%s\n", req.GetTopicId(), req.GetUserId(), req.GetText())

	msg := &pb.Message{
		Id:        s.server_state.GetMsgID(),
		TopicId:   req.GetTopicId(),
		UserId:    req.GetUserId(),
		Text:      req.GetText(),
		CreatedAt: timestamppb.Now(),
		Likes:     0,
	}

	s.server_state.AddMessage(msg)

	// fmt.Printf("PostMessage: ustvarjeno sporočilo id=%d\n", msg.Id)
	return msg, nil
}

func (s *MessageBoardServer) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	msg := s.server_state.GetMessage(req.GetMessageId())
	if msg == nil {
		return nil, status.Error(codes.NotFound, "Sporočilo ne obstaja.")
	}

	if msg.UserId != req.GetUserId() {
		return nil, status.Error(codes.PermissionDenied, "Nimate dovoljenja za urejanje tega sporočila.")
	}

	msg.Text = req.GetText()
	s.server_state.UpdateMessage(msg)

	return msg, nil
}

func (s *MessageBoardServer) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	msg := s.server_state.GetMessage(req.GetMessageId())
	if msg == nil {
		return nil, status.Error(codes.NotFound, "Sporočilo ne obstaja.")
	}

	if msg.UserId != req.GetUserId() {
		return nil, status.Error(codes.PermissionDenied, "Nimate dovoljenja za brisanje tega sporočila.")
	}

	s.server_state.DeleteMessage(req.GetMessageId(), msg.TopicId)

	return &emptypb.Empty{}, nil
}

func (s *MessageBoardServer) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	msg := s.server_state.GetMessage(req.GetMessageId())
	if msg == nil {
		return nil, status.Error(codes.NotFound, "Sporočilo ne obstaja.")
	}

	// TODO more biti pod mutex.
	msg.Likes++
	s.server_state.UpdateMessage(msg)

	return msg, nil
}

func (s *MessageBoardServer) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if len(req.GetTopicId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Ni podanih tem.")
	}

	for _, topicId := range req.GetTopicId() {
		topic := s.server_state.GetSingleTopic(topicId)
		if topic == nil {
			return nil, status.Errorf(codes.NotFound, "Tema %d ne obstaja.", topicId)
		}
	}

	// TODO dinamično moramo dodeljevati nodes. Trenutno harcoded.
	return &pb.SubscriptionNodeResponse{
		SubscribeToken: "token123",
		Node: &pb.NodeInfo{
			NodeId:  "node1",
			Address: "localhost:9876",
		},
	}, nil
}

func (s *MessageBoardServer) ListTopics(ctx context.Context, req *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	topics := s.server_state.GetAllTopics()

	return &pb.ListTopicsResponse{
		Topics: topics,
	}, nil
}

func (s *MessageBoardServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	messages := s.server_state.GetMessagesByTopic(req.GetTopicId())

	return &pb.GetMessagesResponse{
		Messages: messages,
	}, nil
}

func (s *MessageBoardServer) SubscribeTopic(req *pb.SubscribeTopicRequest, stream grpc.ServerStreamingServer[pb.MessageEvent]) error {
	if len(req.GetTopicId()) == 0 {
		return status.Error(codes.InvalidArgument, "Ni podanih tem.")
	}

	// Preverimo, da obstajajo vse teme.
	for _, topicId := range req.GetTopicId() {
		topic := s.server_state.GetSingleTopic(topicId)
		if topic == nil {
			return status.Errorf(codes.NotFound, "Tema %d ne obstaja.", topicId)
		}
	}

	// Clientov slice topicov, na katere je subscriban.
	subscribers := make([]chan *pb.MessageEvent, 0, len(req.GetTopicId()))
	for _, topicId := range req.GetTopicId() {
		subscriber := s.server_state.Subscribe(topicId)
		subscribers = append(subscribers, subscriber)
	}

	// Ko se program konča pospravimo.
	defer func() {
		for i, topicId := range req.GetTopicId() {
			s.server_state.Unsubscribe(topicId, subscribers[i])
		}
	}()

	// TODO trenutno samo prvo posluša: subscribers[0]...
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event := <-subscribers[0]:
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}
