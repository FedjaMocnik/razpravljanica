package client

import (
	"context"
	"fmt"

	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type Odjemalec struct {
	povezava        *grpc.ClientConn
	razpravljalnica pb.MessageBoardClient
	controlPlane    pb.ControlPlaneClient
}

func Povezi(naslov string) (*Odjemalec, error) {
	povezava, napaka := grpc.NewClient(
		naslov,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if napaka != nil {
		return nil, fmt.Errorf("ne morem ustvariti povezave: %w", napaka)
	}

	return &Odjemalec{
		povezava:        povezava,
		razpravljalnica: pb.NewMessageBoardClient(povezava),
		controlPlane:    pb.NewControlPlaneClient(povezava),
	}, nil
}

func (o *Odjemalec) Zapri() error {
	if o.povezava == nil {
		return nil
	}
	return o.povezava.Close()
}

func (o *Odjemalec) CreateUser(ctx context.Context, ime string) (*pb.User, error) {
	return o.razpravljalnica.CreateUser(ctx, &pb.CreateUserRequest{Name: ime})
}

func (o *Odjemalec) CreateTopic(ctx context.Context, ime string) (*pb.Topic, error) {
	return o.razpravljalnica.CreateTopic(ctx, &pb.CreateTopicRequest{Name: ime})
}

func (o *Odjemalec) PostMessage(ctx context.Context, temaID, uporabnikID int64, besedilo string) (*pb.Message, error) {
	return o.razpravljalnica.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: temaID,
		UserId:  uporabnikID,
		Text:    besedilo,
	})
}

func (o *Odjemalec) UpdateMessage(ctx context.Context, temaID, uporabnikID, sporociloID int64, besedilo string) (*pb.Message, error) {
	return o.razpravljalnica.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   temaID,
		UserId:    uporabnikID,
		MessageId: sporociloID,
		Text:      besedilo,
	})
}

func (o *Odjemalec) DeleteMessage(ctx context.Context, temaID, uporabnikID, sporociloID int64) error {
	_, napaka := o.razpravljalnica.DeleteMessage(ctx, &pb.DeleteMessageRequest{
		TopicId:   temaID,
		UserId:    uporabnikID,
		MessageId: sporociloID,
	})
	return napaka
}

func (o *Odjemalec) LikeMessage(ctx context.Context, temaID, uporabnikID, sporociloID int64) (*pb.Message, error) {
	return o.razpravljalnica.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   temaID,
		UserId:    uporabnikID,
		MessageId: sporociloID,
	})
}

func (o *Odjemalec) ListTopics(ctx context.Context) (*pb.ListTopicsResponse, error) {
	return o.razpravljalnica.ListTopics(ctx, &emptypb.Empty{})
}

func (o *Odjemalec) GetMessages(ctx context.Context, temaID, fromID int64, limit int32) (*pb.GetMessagesResponse, error) {
	return o.razpravljalnica.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       temaID,
		FromMessageId: fromID,
		Limit:         limit,
	})
}

func (o *Odjemalec) GetClusterState(ctx context.Context) (*pb.GetClusterStateResponse, error) {
	return o.controlPlane.GetClusterState(ctx, &emptypb.Empty{})
}

// Naročnina: najprej dobi node+token, potem odpre stream.
func (o *Odjemalec) Subscribe(ctx context.Context, uporabnikID int64, teme []int64, fromID int64) (pb.MessageBoard_SubscribeTopicClient, error) {
	odgovor, napaka := o.razpravljalnica.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  uporabnikID,
		TopicId: teme,
	})
	if napaka != nil {
		return nil, napaka
	}

	// Če bi bil cluster, bi se tu povezal na odgovor.Node.Address.
	// Ker je 1 strežnik, ostanemo na isti povezavi.
	// TODO: Logika za več strežnikov.
	stream, napaka := o.razpravljalnica.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
		UserId:         uporabnikID,
		TopicId:        teme,
		FromMessageId:  fromID,
		SubscribeToken: odgovor.SubscribeToken,
	})
	if napaka != nil {
		return nil, napaka
	}

	return stream, nil
}
