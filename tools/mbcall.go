package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	addr := flag.String("addr", "", "gRPC naslov data node-a (npr. localhost:9876)")
	flag.Parse()

	if *addr == "" || flag.NArg() < 1 {
		fmt.Println(`usage:
  go run tools/mbcall.go --addr host:port create-topic --name "TemaX"
  go run tools/mbcall.go --addr host:port list-topics
  go run tools/mbcall.go --addr host:port get-subnode --user 2 --topics "1,2"
  go run tools/mbcall.go --addr host:port subscribe-once --user 2 --topics "1" --from 0 --token "<TOKEN>"`)
		os.Exit(2)
	}

	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	mb := publicpb.NewMessageBoardClient(conn)

	switch cmd {

	case "create-topic":
		fs := flag.NewFlagSet("create-topic", flag.ExitOnError)
		name := fs.String("name", "", "ime teme")
		_ = fs.Parse(args)
		if *name == "" {
			fmt.Println("missing --name")
			os.Exit(2)
		}
		t, err := mb.CreateTopic(ctx, &publicpb.CreateTopicRequest{Name: *name})
		printResult("CreateTopic", err)
		if err == nil {
			fmt.Printf("OK topic_id=%d name=%q\n", t.GetId(), t.GetName())
		}

	case "list-topics":
		resp, err := mb.ListTopics(ctx, &emptypb.Empty{})
		printResult("ListTopics", err)
		if err == nil {
			fmt.Printf("OK topics=%d\n", len(resp.GetTopics()))
			for _, t := range resp.GetTopics() {
				fmt.Printf("  - id=%d name=%q\n", t.GetId(), t.GetName())
			}
		}

	case "get-subnode":
		fs := flag.NewFlagSet("get-subnode", flag.ExitOnError)
		user := fs.Int64("user", 0, "user id")
		topics := fs.String("topics", "", "CSV topic ids (npr. 1,2)")
		_ = fs.Parse(args)
		if *user == 0 || *topics == "" {
			fmt.Println("missing --user or --topics")
			os.Exit(2)
		}
		tids := parseCSV(*topics)
		resp, err := mb.GetSubscriptionNode(ctx, &publicpb.SubscriptionNodeRequest{
			UserId:  *user,
			TopicId: tids,
		})
		printResult("GetSubscriptionNode", err)
		if err == nil {
			fmt.Printf("OK subscribe_token=%q assigned_node=%s (%s)\n",
				resp.GetSubscribeToken(),
				resp.GetNode().GetNodeId(),
				resp.GetNode().GetAddress(),
			)
		}

	case "subscribe-once":
		fs := flag.NewFlagSet("subscribe-once", flag.ExitOnError)
		user := fs.Int64("user", 0, "user id")
		topics := fs.String("topics", "", "CSV topic ids")
		from := fs.Int64("from", 0, "from_message_id")
		token := fs.String("token", "", "subscribe token")
		_ = fs.Parse(args)
		if *user == 0 || *topics == "" || *token == "" {
			fmt.Println("missing --user or --topics or --token")
			os.Exit(2)
		}
		tids := parseCSV(*topics)

		stream, err := mb.SubscribeTopic(context.Background(), &publicpb.SubscribeTopicRequest{
			UserId:         *user,
			TopicId:        tids,
			FromMessageId:  *from,
			SubscribeToken: *token,
		})
		if err != nil {
			printResult("SubscribeTopic (open)", err)
			return
		}

		recvCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		ch := make(chan error, 1)
		var ev *publicpb.MessageEvent
		go func() {
			e, eerr := stream.Recv()
			ev = e
			ch <- eerr
		}()

		select {
		case eerr := <-ch:
			printResult("SubscribeTopic (recv)", eerr)
			if eerr == nil && ev != nil && ev.GetMessage() != nil {
				fmt.Printf("OK got event seq=%d op=%s msg_id=%d\n", ev.GetSequenceNumber(), ev.GetOp().String(), ev.GetMessage().GetId())
			}
		case <-recvCtx.Done():
			fmt.Println("OK stream opened (no event within 3s) -> this is fine for this test")
		}

	default:
		fmt.Println("unknown command:", cmd)
		os.Exit(2)
	}
}

func parseCSV(s string) []int64 {
	parts := strings.Split(s, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var x int64
		fmt.Sscan(p, &x)
		out = append(out, x)
	}
	return out
}

func printResult(op string, err error) {
	if err == nil {
		return
	}
	if st, ok := status.FromError(err); ok {
		fmt.Printf("ERROR %s code=%s msg=%q\n", op, st.Code().String(), st.Message())
		return
	}
	fmt.Printf("ERROR %s err=%v\n", op, err)
}
