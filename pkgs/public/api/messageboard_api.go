package api

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/FedjaMocnik/razpravljalnica/internal/replication"
	"github.com/FedjaMocnik/razpravljalnica/internal/storage"
	"github.com/FedjaMocnik/razpravljalnica/internal/subtoken"
	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type MessageBoardServer struct {
	pb.UnimplementedMessageBoardServer

	stanje          *storage.State
	podatkiVozlisca *pb.NodeInfo
	veriga          []*pb.NodeInfo

	isHead bool
	isTail bool

	writeMu sync.Mutex

	repl        *replication.Manager
	tokenSecret []byte
}

type NodeOptions struct {
	State       *storage.State
	NodeInfo    *pb.NodeInfo
	Chain       []*pb.NodeInfo
	IsHead      bool
	IsTail      bool
	Replicator  *replication.Manager
	TokenSecret []byte
}

func NewMessageBoardServer(opts NodeOptions) *MessageBoardServer {
	st := opts.State
	if st == nil {
		st = storage.NewState()
	}
	return &MessageBoardServer{
		stanje:          st,
		podatkiVozlisca: opts.NodeInfo,
		veriga:          opts.Chain,
		isHead:          opts.IsHead,
		isTail:          opts.IsTail,
		repl:            opts.Replicator,
		tokenSecret:     opts.TokenSecret,
	}
}

func (s *MessageBoardServer) requireHead() error {
	if s.isHead {
		return nil
	}
	return status.Error(codes.FailedPrecondition, "Operacija je dovoljena samo na HEAD vozlišču.")
}

func (s *MessageBoardServer) requireTail() error {
	if !s.isTail {
		return status.Error(codes.FailedPrecondition, "Operacija je dovoljena samo na TAIL vozlišču.")
	}

	if s.repl != nil && !s.repl.IsReady() {
		return status.Error(codes.Unavailable, "Vozlišče se še sinhronizira (bootstrap) – poskusite ponovno čez trenutek.")
	}
	return nil
}

func (s *MessageBoardServer) hashSubscriptionKey(userID int64, topicIDs []int64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprintf("u:%d;", userID)))
	for _, tid := range topicIDs {
		_, _ = h.Write([]byte(fmt.Sprintf("t:%d;", tid)))
	}
	return h.Sum64()
}

func (s *MessageBoardServer) pickSubscriptionNode(userID int64, topicIDs []int64) *pb.NodeInfo {
	if len(s.veriga) == 0 {
		return s.podatkiVozlisca
	}
	k := s.hashSubscriptionKey(userID, topicIDs)
	idx := int(k % uint64(len(s.veriga)))
	return s.veriga[idx]
}

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if err := s.requireHead(); err != nil {
		return nil, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	uporabnik := &pb.User{Id: s.stanje.GetUserID(), Name: req.GetName()}
	if s.repl != nil {
		// Prvo replikacija. Potem apliciramo v lokalno bazo po commitu.
		if err := s.repl.ReplicateUser(ctx, uporabnik); err != nil {
			return nil, status.Error(codes.Internal, "Napaka pri replikaciji uporabnika.")
		}
	}
	s.stanje.UpsertUser(uporabnik)
	return uporabnik, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if err := s.requireHead(); err != nil {
		return nil, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tema := &pb.Topic{Id: s.stanje.GetTopicID(), Name: req.GetName()}
	if s.repl != nil {

		if err := s.repl.ReplicateTopic(ctx, tema); err != nil {
			return nil, status.Error(codes.Internal, "Napaka pri replikaciji teme.")
		}
	}
	s.stanje.UpsertTopic(tema)
	return tema, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if err := s.requireHead(); err != nil {
		return nil, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := timestamppb.Now()
	msg, err := s.stanje.PreparePostMessage(req.GetTopicId(), req.GetUserId(), req.GetText(), now)
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
	seq := s.stanje.NextSequenceNumber()
	ev := &pb.MessageEvent{SequenceNumber: seq, Op: pb.OpType_OP_POST, Message: msg, EventAt: now}
	if s.repl != nil {

		if err := s.repl.ReplicateMessageEvent(ctx, ev); err != nil {
			return nil, status.Error(codes.Internal, "Napaka pri replikaciji sporočila.")
		}
	}
	if err := s.stanje.ApplyReplicatedEvent(ev); err != nil {
		return nil, status.Error(codes.Internal, "Napaka pri uveljavitvi sporočila.")
	}
	return msg, nil
}

func (s *MessageBoardServer) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	if err := s.requireHead(); err != nil {
		return nil, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	updated, err := s.stanje.PrepareUpdateMessageText(req.GetTopicId(), req.GetUserId(), req.GetMessageId(), req.GetText())
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
	now := timestamppb.Now()
	seq := s.stanje.NextSequenceNumber()
	ev := &pb.MessageEvent{SequenceNumber: seq, Op: pb.OpType_OP_UPDATE, Message: updated, EventAt: now}
	if s.repl != nil {

		if err := s.repl.ReplicateMessageEvent(ctx, ev); err != nil {
			return nil, status.Error(codes.Internal, "Napaka pri replikaciji posodobitve.")
		}
	}
	if err := s.stanje.ApplyReplicatedEvent(ev); err != nil {
		return nil, status.Error(codes.Internal, "Napaka pri uveljavitvi posodobitve.")
	}
	return updated, nil
}

func (s *MessageBoardServer) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	if err := s.requireHead(); err != nil {
		return nil, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	snapshot, err := s.stanje.PrepareDeleteMessageIfOwner(req.GetTopicId(), req.GetUserId(), req.GetMessageId())
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
	now := timestamppb.Now()
	seq := s.stanje.NextSequenceNumber()
	ev := &pb.MessageEvent{SequenceNumber: seq, Op: pb.OpType_OP_DELETE, Message: snapshot, EventAt: now}
	if s.repl != nil {

		if err := s.repl.ReplicateMessageEvent(ctx, ev); err != nil {
			return nil, status.Error(codes.Internal, "Napaka pri replikaciji brisanja.")
		}
	}
	if err := s.stanje.ApplyReplicatedEvent(ev); err != nil {
		return nil, status.Error(codes.Internal, "Napaka pri uveljavitvi brisanja.")
	}
	return &emptypb.Empty{}, nil
}

func (s *MessageBoardServer) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	if err := s.requireHead(); err != nil {
		return nil, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.stanje.GetUser(req.GetUserId()) == nil {
		return nil, status.Error(codes.NotFound, "Uporabnik ne obstaja.")
	}
	updated, changed, err := s.stanje.PrepareLikeMessage(req.GetTopicId(), req.GetMessageId(), req.GetUserId())
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

	if !changed {
		return updated, nil
	}
	now := timestamppb.Now()
	seq := s.stanje.NextSequenceNumber()
	ev := &pb.MessageEvent{SequenceNumber: seq, Op: pb.OpType_OP_LIKE, Message: updated, EventAt: now}
	if s.repl != nil {

		if err := s.repl.ReplicateMessageEvent(ctx, ev); err != nil {
			return nil, status.Error(codes.Internal, "Napaka pri replikaciji všečka.")
		}
	}
	if err := s.stanje.ApplyReplicatedEvent(ev); err != nil {
		return nil, status.Error(codes.Internal, "Napaka pri uveljavitvi všečka.")
	}

	s.stanje.MarkLike(req.GetTopicId(), req.GetMessageId(), req.GetUserId())
	return updated, nil
}

func (s *MessageBoardServer) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if err := s.requireHead(); err != nil {
		return nil, err
	}
	if len(req.GetTopicId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Ni podanih tem.")
	}

	canonicalTopicIDs := append([]int64(nil), req.GetTopicId()...)
	sort.Slice(canonicalTopicIDs, func(i, j int) bool { return canonicalTopicIDs[i] < canonicalTopicIDs[j] })

	uporabnik := s.stanje.GetUser(req.GetUserId())
	if uporabnik == nil {
		return nil, status.Error(codes.NotFound, "Uporabnik ne obstaja.")
	}

	for _, temaID := range canonicalTopicIDs {
		tema := s.stanje.GetSingleTopic(temaID)
		if tema == nil {
			return nil, status.Errorf(codes.NotFound, "Tema %d ne obstaja.", temaID)
		}
	}

	node := s.pickSubscriptionNode(req.GetUserId(), canonicalTopicIDs)
	if node == nil {
		node = s.podatkiVozlisca
	}

	tok, err := subtoken.Make(s.tokenSecret, subtoken.Claims{
		UserID:   req.GetUserId(),
		TopicIDs: canonicalTopicIDs,
		NodeID:   node.GetNodeId(),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "Napaka pri generiranju subscribe tokena.")
	}

	return &pb.SubscriptionNodeResponse{SubscribeToken: tok, Node: node}, nil
}

func (s *MessageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	if err := s.requireTail(); err != nil {
		return nil, err
	}
	teme := s.stanje.GetAllTopics()
	return &pb.ListTopicsResponse{Topics: teme}, nil
}

func (s *MessageBoardServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if err := s.requireTail(); err != nil {
		return nil, err
	}
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

	claims, err := subtoken.ParseAndVerify(s.tokenSecret, req.GetSubscribeToken())
	if err != nil {
		return status.Error(codes.PermissionDenied, "Neveljaven subscribe token.")
	}
	if claims.UserID != req.GetUserId() {
		return status.Error(codes.PermissionDenied, "Neveljaven subscribe token.")
	}
	if claims.NodeID != s.podatkiVozlisca.GetNodeId() {
		return status.Error(codes.PermissionDenied, "Naročnina mora biti odprta na pravilnem vozlišču.")
	}

	allowed := map[int64]struct{}{}
	for _, id := range claims.TopicIDs {
		allowed[id] = struct{}{}
	}
	for _, id := range req.GetTopicId() {
		if _, ok := allowed[id]; !ok {
			return status.Error(codes.PermissionDenied, "Neveljaven subscribe token.")
		}
	}

	if s.repl != nil && !s.repl.IsReady() {
		return status.Error(codes.Unavailable, "Vozlišče se še sinhronizira (bootstrap) – poskusite ponovno čez trenutek.")
	}

	for _, temaID := range req.GetTopicId() {
		if s.stanje.GetSingleTopic(temaID) == nil {
			return status.Errorf(codes.NotFound, "Tema %d ne obstaja.", temaID)
		}
	}

	// POPRAVEK: naročnina ima EN kanal in globalni event log poskrbi za history.
	nID, narocniskiKanal, zgodovinaDogodkov := s.stanje.SubscribeTopicsWithHistory(req.GetTopicId(), req.GetFromMessageId())
	defer s.stanje.Unsubscribe(nID)

	ctx := stream.Context()

	fromID := req.GetFromMessageId()

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
				if ev == nil {
					continue
				}
				if fromID > 0 {
					m := ev.GetMessage()
					if m == nil || m.GetId() < fromID {
						continue
					}
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

	// 1 Pošlji zgodovino (iz globalnega event loga; sequence_number ostane pravi).
	for _, dogodek := range zgodovinaDogodkov {
		if err := stream.Send(dogodek); err != nil {
			return err
		}
	}

	// 2 Zdaj dovoli pošiljanje live dogodkov (flush + nadaljevanje).
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
