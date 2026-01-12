package client

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type Odjemalec struct {
	controlAddr string
	controlConn *grpc.ClientConn
	control     pb.ControlPlaneClient

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	mbs   map[string]pb.MessageBoardClient
}

func Povezi(controlAddr string) (*Odjemalec, error) {
	cc, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("ne morem ustvariti povezave: %w", err)
	}
	return &Odjemalec{
		controlAddr: controlAddr,
		controlConn: cc,
		control:     pb.NewControlPlaneClient(cc),
		conns:       make(map[string]*grpc.ClientConn),
		mbs:         make(map[string]pb.MessageBoardClient),
	}, nil
}

func (o *Odjemalec) Zapri() error {
	o.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(o.conns)+1)
	for _, c := range o.conns {
		conns = append(conns, c)
	}
	o.conns = map[string]*grpc.ClientConn{}
	o.mbs = map[string]pb.MessageBoardClient{}
	cc := o.controlConn
	o.controlConn = nil
	o.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
	if cc != nil {
		return cc.Close()
	}
	return nil
}

func (o *Odjemalec) getClusterState(ctx context.Context) (*pb.GetClusterStateResponse, error) {
	return o.control.GetClusterState(ctx, &emptypb.Empty{})
}

// Pod ključavnico naredimo client, da ne zaženemo dveh grpc povezav.
func (o *Odjemalec) mbClient(addr string) (pb.MessageBoardClient, error) {
	o.mu.Lock()
	if c, ok := o.mbs[addr]; ok {
		o.mu.Unlock()
		return c, nil
	}
	o.mu.Unlock()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	cli := pb.NewMessageBoardClient(cc)

	o.mu.Lock()

	if existing, ok := o.mbs[addr]; ok {
		o.mu.Unlock()
		_ = cc.Close()
		return existing, nil
	}
	o.conns[addr] = cc
	o.mbs[addr] = cli
	o.mu.Unlock()
	return cli, nil
}

func (o *Odjemalec) headClient(ctx context.Context) (pb.MessageBoardClient, error) {
	st, err := o.getClusterState(ctx)
	if err != nil {
		return nil, err
	}
	return o.mbClient(st.GetHead().GetAddress())
}

func (o *Odjemalec) tailClient(ctx context.Context) (pb.MessageBoardClient, error) {
	st, err := o.getClusterState(ctx)
	if err != nil {
		return nil, err
	}
	return o.mbClient(st.GetTail().GetAddress())
}

func (o *Odjemalec) CreateUser(ctx context.Context, ime string) (*pb.User, error) {
	cli, err := o.headClient(ctx)
	if err != nil {
		return nil, err
	}
	return cli.CreateUser(ctx, &pb.CreateUserRequest{Name: ime})
}

func (o *Odjemalec) CreateTopic(ctx context.Context, ime string) (*pb.Topic, error) {
	cli, err := o.headClient(ctx)
	if err != nil {
		return nil, err
	}
	return cli.CreateTopic(ctx, &pb.CreateTopicRequest{Name: ime})
}

func (o *Odjemalec) PostMessage(ctx context.Context, temaID, uporabnikID int64, besedilo string) (*pb.Message, error) {
	cli, err := o.headClient(ctx)
	if err != nil {
		return nil, err
	}
	return cli.PostMessage(ctx, &pb.PostMessageRequest{TopicId: temaID, UserId: uporabnikID, Text: besedilo})
}

func (o *Odjemalec) UpdateMessage(ctx context.Context, temaID, uporabnikID, sporociloID int64, besedilo string) (*pb.Message, error) {
	cli, err := o.headClient(ctx)
	if err != nil {
		return nil, err
	}
	return cli.UpdateMessage(ctx, &pb.UpdateMessageRequest{TopicId: temaID, UserId: uporabnikID, MessageId: sporociloID, Text: besedilo})
}

func (o *Odjemalec) DeleteMessage(ctx context.Context, temaID, uporabnikID, sporociloID int64) error {
	cli, err := o.headClient(ctx)
	if err != nil {
		return err
	}
	_, err = cli.DeleteMessage(ctx, &pb.DeleteMessageRequest{TopicId: temaID, UserId: uporabnikID, MessageId: sporociloID})
	return err
}

func (o *Odjemalec) LikeMessage(ctx context.Context, temaID, uporabnikID, sporociloID int64) (*pb.Message, error) {
	cli, err := o.headClient(ctx)
	if err != nil {
		return nil, err
	}
	return cli.LikeMessage(ctx, &pb.LikeMessageRequest{TopicId: temaID, UserId: uporabnikID, MessageId: sporociloID})
}

func (o *Odjemalec) ListTopics(ctx context.Context) (*pb.ListTopicsResponse, error) {
	cli, err := o.tailClient(ctx)
	if err != nil {
		return nil, err
	}
	return cli.ListTopics(ctx, &emptypb.Empty{})
}

func (o *Odjemalec) GetMessages(ctx context.Context, temaID, fromID int64, limit int32) (*pb.GetMessagesResponse, error) {
	cli, err := o.tailClient(ctx)
	if err != nil {
		return nil, err
	}
	return cli.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: temaID, FromMessageId: fromID, Limit: limit})
}

func (o *Odjemalec) GetClusterState(ctx context.Context) (*pb.GetClusterStateResponse, error) {
	return o.getClusterState(ctx)
}

func (o *Odjemalec) Subscribe(ctx context.Context, uporabnikID int64, teme []int64, fromID int64) (pb.MessageBoard_SubscribeTopicClient, error) {
	head, err := o.headClient(ctx)
	if err != nil {
		return nil, err
	}
	odgovor, err := head.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: uporabnikID, TopicId: teme})
	if err != nil {
		return nil, err
	}
	nodeAddr := odgovor.GetNode().GetAddress()
	cli, err := o.mbClient(nodeAddr)
	if err != nil {
		return nil, err
	}
	return cli.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
		UserId:         uporabnikID,
		TopicId:        teme,
		FromMessageId:  fromID,
		SubscribeToken: odgovor.GetSubscribeToken(),
	})
}
