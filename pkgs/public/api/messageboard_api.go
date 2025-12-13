package api

import (
	"context"

	storage "github.com/FedjaMocnik/razpravljalnica/internal/storage"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MessageBoardServer struct {
	pb.UnimplementedMessageBoardServer

	stanje          *storage.State
	podatkiVozlisca *pb.NodeInfo
}

func NewMessageBoardServer() *MessageBoardServer {
	return &MessageBoardServer{
		stanje: storage.NewState(),
		podatkiVozlisca: &pb.NodeInfo{
			NodeId:  "node1",
			Address: "localhost:9876",
		},
	}
}

// Za pravi zagon strežnika (da se v response vrača pravilen address).
func NewMessageBoardServerZNaslovom(naslov string) *MessageBoardServer {
	s := NewMessageBoardServer()
	s.podatkiVozlisca.Address = naslov
	return s
}

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	uporabnik := &pb.User{
		Id:   s.stanje.GetUserID(),
		Name: req.GetName(),
	}
	s.stanje.AddUser(uporabnik)
	return uporabnik, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	tema := &pb.Topic{
		Id:   s.stanje.GetTopicID(),
		Name: req.GetName(),
	}
	s.stanje.AddTopic(tema)
	return tema, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	sporocilo, err := s.stanje.CreateMessage(req.GetTopicId(), req.GetUserId(), req.GetText())
	if err != nil {
		switch err {
		case storage.ErrUserNotFound:
			return nil, status.Error(codes.NotFound, "Uporabnik ne obstaja.")
		case storage.ErrTopicNotFound:
			return nil, status.Error(codes.NotFound, "Tema ne obstaja.")
		default:
			return nil, status.Error(codes.Internal, "Napaka pri objavi sporočila.")
		}
	}
	return sporocilo, nil
}

func (s *MessageBoardServer) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	posodobljeno, err := s.stanje.UpdateMessageText(req.GetTopicId(), req.GetUserId(), req.GetMessageId(), req.GetText())
	if err != nil {
		switch err {
		case storage.ErrMessageNotFound:
			return nil, status.Error(codes.NotFound, "Sporočilo ne obstaja.")
		case storage.ErrTopicMismatch:
			return nil, status.Error(codes.InvalidArgument, "Sporočilo ne pripada podani temi.")
		case storage.ErrNotOwner:
			return nil, status.Error(codes.PermissionDenied, "Nimate dovoljenja za urejanje tega sporočila.")
		default:
			return nil, status.Error(codes.Internal, "Napaka pri posodobitvi sporočila.")
		}
	}
	return posodobljeno, nil
}

func (s *MessageBoardServer) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	err := s.stanje.DeleteMessageIfOwner(req.GetTopicId(), req.GetUserId(), req.GetMessageId())
	if err != nil {
		switch err {
		case storage.ErrMessageNotFound:
			return nil, status.Error(codes.NotFound, "Sporočilo ne obstaja.")
		case storage.ErrTopicMismatch:
			return nil, status.Error(codes.InvalidArgument, "Sporočilo ne pripada podani temi.")
		case storage.ErrNotOwner:
			return nil, status.Error(codes.PermissionDenied, "Nimate dovoljenja za brisanje tega sporočila.")
		default:
			return nil, status.Error(codes.Internal, "Napaka pri brisanju sporočila.")
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *MessageBoardServer) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	// user_id v LikeMessageRequest predstavlja uporabnika, ki je všečkal sporočilo.
	if s.stanje.GetUser(req.GetUserId()) == nil {
		return nil, status.Error(codes.NotFound, "Uporabnik ne obstaja.")
	}

	posodobljeno, err := s.stanje.LikeMessageInTopic(req.GetTopicId(), req.GetMessageId(), req.GetUserId())
	if err != nil {
		switch err {
		case storage.ErrMessageNotFound:
			return nil, status.Error(codes.NotFound, "Sporočilo ne obstaja.")
		case storage.ErrTopicMismatch:
			return nil, status.Error(codes.InvalidArgument, "Sporočilo ne pripada podani temi.")
		default:
			return nil, status.Error(codes.Internal, "Napaka pri všečkanju sporočila.")
		}
	}
	return posodobljeno, nil
}

func (s *MessageBoardServer) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if len(req.GetTopicId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Ni podanih tem.")
	}

	uporabnik := s.stanje.GetUser(req.GetUserId())
	if uporabnik == nil {
		return nil, status.Error(codes.NotFound, "Uporabnik ne obstaja.")
	}

	for _, temaID := range req.GetTopicId() {
		tema := s.stanje.GetSingleTopic(temaID)
		if tema == nil {
			return nil, status.Errorf(codes.NotFound, "Tema %d ne obstaja.", temaID)
		}
	}

	zeton := s.stanje.UstvariSubscribeToken(req.GetUserId(), req.GetTopicId())

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: zeton,
		Node:           s.podatkiVozlisca,
	}, nil
}

func (s *MessageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	teme := s.stanje.GetAllTopics()
	return &pb.ListTopicsResponse{Topics: teme}, nil
}

func (s *MessageBoardServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	tema := s.stanje.GetSingleTopic(req.GetTopicId())
	if tema == nil {
		return nil, status.Error(codes.NotFound, "Tema ne obstaja.")
	}

	sporocila := s.stanje.GetMessagesByTopic(req.GetTopicId(), req.GetFromMessageId(), req.GetLimit())
	return &pb.GetMessagesResponse{Messages: sporocila}, nil
}

func (s *MessageBoardServer) SubscribeTopic(req *pb.SubscribeTopicRequest, stream grpc.ServerStreamingServer[pb.MessageEvent]) error {
	if len(req.GetTopicId()) == 0 {
		return status.Error(codes.InvalidArgument, "Ni podanih tem.")
	}

	// Token preverjanje (glava -> token -> tail; pri 1 strežniku je to isto).
	if !s.stanje.PreveriSubscribeToken(req.GetSubscribeToken(), req.GetUserId(), req.GetTopicId()) {
		return status.Error(codes.PermissionDenied, "Neveljaven subscribe token.")
	}

	// Preverimo, da teme obstajajo.
	for _, temaID := range req.GetTopicId() {
		if s.stanje.GetSingleTopic(temaID) == nil {
			return status.Errorf(codes.NotFound, "Tema %d ne obstaja.", temaID)
		}
	}

	// POPRAVEK: naročnina ima EN kanal in globalni event log poskrbi za history.
	nID, narocniskiKanal, zgodovinaDogodkov := s.stanje.SubscribeTopicsWithHistory(req.GetTopicId(), req.GetFromMessageId())
	defer s.stanje.Unsubscribe(nID)

	ctx := stream.Context()

	// Live dogodke bufferiramo, dokler ne pošljemo zgodovine, da se naročniški kanal ne napolni.
	startLive := make(chan struct{})
	dostava := make(chan *pb.MessageEvent, 256)

	go func() {
		defer close(dostava)
		var buf []*pb.MessageEvent
		startCh := startLive

		for {
			select {
			case <-ctx.Done():
				return
			case <-startCh:
				startCh = nil
				for _, ev := range buf {
					select {
					case dostava <- ev:
					case <-ctx.Done():
						return
					}
				}
				buf = nil
			case ev, ok := <-narocniskiKanal:
				if !ok {
					return
				}
				if startCh != nil {
					buf = append(buf, ev)
					continue
				}
				select {
				case dostava <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// 1 | Pošlji zgodovino (iz globalnega event loga; sequence_number ostane pravi).
	for _, dogodek := range zgodovinaDogodkov {
		if err := stream.Send(dogodek); err != nil {
			return err
		}
	}

	// 2 | Zdaj dovoli pošiljanje live dogodkov (flush + nadaljevanje).
	close(startLive)

	for {
		select {
		case <-ctx.Done():
			return nil
		case dogodek, ok := <-dostava:
			if !ok {
				return nil
			}
			if err := stream.Send(dogodek); err != nil {
				return err
			}
		}
	}
}
