package api

import (
	"context"
	// "io"
	"testing"
	// "time"

	"github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateUser(t *testing.T) {
	s := NewMessageBoardServer()
	user, err := s.CreateUser(context.Background(), &pb.CreateUserRequest{
		Name: "Alice",
	})
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
	if user.Id != 1 {
		t.Errorf("expected ID 1, got %d", user.Id)
	}
	if user.Name != "Alice" {
		t.Errorf("expected name Alice, got %s", user.Name)
	}
}

func TestCreateMultipleUsers(t *testing.T) {
	s := NewMessageBoardServer()

	user1, err := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	user2, err := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Bob"})
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	if user1.Id == user2.Id {
		t.Errorf("expected different IDs, got both %d", user1.Id)
	}

	if user2.Id != user1.Id+1 {
		t.Errorf("expected sequential IDs, got %d and %d", user1.Id, user2.Id)
	}
}

func TestCreateTopic(t *testing.T) {
	s := NewMessageBoardServer()

	topic, err := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Name: "General",
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	if topic.Id != 1 {
		t.Errorf("expected ID 1, got %d", topic.Id)
	}
	if topic.Name != "General" {
		t.Errorf("expected name General, got %s", topic.Name)
	}
}

func TestCreateMultipleTopics(t *testing.T) {
	s := NewMessageBoardServer()

	topic1, err := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	topic2, err := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "Random"})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	if topic1.Id == topic2.Id {
		t.Errorf("expected different IDs, got both %d", topic1.Id)
	}

	if topic2.Id != topic1.Id+1 {
		t.Errorf("expected sequential IDs, got %d and %d", topic1.Id, topic2.Id)
	}
}

func TestPostMessage(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})

	msg, err := s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Hello World",
	})

	if err != nil {
		t.Fatalf("PostMessage failed: %v", err)
	}
	if msg.Id != 1 {
		t.Errorf("expected ID 1, got %d", msg.Id)
	}
	if msg.TopicId != topic.Id {
		t.Errorf("expected topic ID %d, got %d", topic.Id, msg.TopicId)
	}
	if msg.UserId != user.Id {
		t.Errorf("expected user ID %d, got %d", user.Id, msg.UserId)
	}
	if msg.Text != "Hello World" {
		t.Errorf("expected text 'Hello World', got %s", msg.Text)
	}
	if msg.Likes != 0 {
		t.Errorf("expected 0 likes, got %d", msg.Likes)
	}
	if msg.CreatedAt == nil {
		t.Error("expected CreatedAt to be set")
	}
}

func TestUpdateMessage(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	msg, _ := s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Original text",
	})

	updated, err := s.UpdateMessage(context.Background(), &pb.UpdateMessageRequest{
		TopicId:   topic.Id,
		UserId:    user.Id,
		MessageId: msg.Id,
		Text:      "Updated text",
	})

	if err != nil {
		t.Fatalf("UpdateMessage failed: %v", err)
	}
	if updated.Text != "Updated text" {
		t.Errorf("expected text 'Updated text', got %s", updated.Text)
	}
	if updated.Id != msg.Id {
		t.Errorf("expected same ID %d, got %d", msg.Id, updated.Id)
	}
}

func TestUpdateMessageNotFound(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})

	_, err := s.UpdateMessage(context.Background(), &pb.UpdateMessageRequest{
		TopicId:   topic.Id,
		UserId:    user.Id,
		MessageId: 999,
		Text:      "Updated text",
	})

	if err == nil {
		t.Fatal("expected error for non-existent message")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", st.Code())
	}
}

func TestUpdateMessagePermissionDenied(t *testing.T) {
	s := NewMessageBoardServer()

	user1, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	user2, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Bob"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	msg, _ := s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user1.Id,
		Text:    "Original text",
	})

	_, err := s.UpdateMessage(context.Background(), &pb.UpdateMessageRequest{
		TopicId:   topic.Id,
		UserId:    user2.Id,
		MessageId: msg.Id,
		Text:      "Hacked text",
	})

	if err == nil {
		t.Fatal("expected permission denied error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied code, got %v", st.Code())
	}
}

func TestDeleteMessage(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	msg, _ := s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "To be deleted",
	})

	_, err := s.DeleteMessage(context.Background(), &pb.DeleteMessageRequest{
		TopicId:   topic.Id,
		UserId:    user.Id,
		MessageId: msg.Id,
	})

	if err != nil {
		t.Fatalf("DeleteMessage failed: %v", err)
	}

	messages, _ := s.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: topic.Id,
	})

	if len(messages.Messages) != 0 {
		t.Errorf("expected 0 messages after deletion, got %d", len(messages.Messages))
	}
}

func TestDeleteMessageNotFound(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})

	_, err := s.DeleteMessage(context.Background(), &pb.DeleteMessageRequest{
		TopicId:   topic.Id,
		UserId:    user.Id,
		MessageId: 999,
	})

	if err == nil {
		t.Fatal("expected error for non-existent message")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", st.Code())
	}
}

func TestDeleteMessagePermissionDenied(t *testing.T) {
	s := NewMessageBoardServer()

	user1, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	user2, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Bob"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	msg, _ := s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user1.Id,
		Text:    "Alice's message",
	})

	_, err := s.DeleteMessage(context.Background(), &pb.DeleteMessageRequest{
		TopicId:   topic.Id,
		UserId:    user2.Id,
		MessageId: msg.Id,
	})

	if err == nil {
		t.Fatal("expected permission denied error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied code, got %v", st.Code())
	}
}

func TestLikeMessage(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	msg, _ := s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Like this!",
	})

	liked, err := s.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		TopicId:   topic.Id,
		MessageId: msg.Id,
		UserId:    user.Id,
	})

	if err != nil {
		t.Fatalf("LikeMessage failed: %v", err)
	}
	if liked.Likes != 1 {
		t.Errorf("expected 1 like, got %d", liked.Likes)
	}

	liked, _ = s.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		TopicId:   topic.Id,
		MessageId: msg.Id,
		UserId:    user.Id,
	})

	if liked.Likes != 2 {
		t.Errorf("expected 2 likes, got %d", liked.Likes)
	}
}

func TestLikeMessageNotFound(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})

	_, err := s.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		TopicId:   topic.Id,
		MessageId: 999,
		UserId:    user.Id,
	})

	if err == nil {
		t.Fatal("expected error for non-existent message")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", st.Code())
	}
}

func TestListTopics(t *testing.T) {
	s := NewMessageBoardServer()

	s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "Random"})
	s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "Tech"})

	response, err := s.ListTopics(context.Background(), nil)

	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	if len(response.Topics) != 3 {
		t.Errorf("expected 3 topics, got %d", len(response.Topics))
	}
}

func TestListTopicsEmpty(t *testing.T) {
	s := NewMessageBoardServer()

	response, err := s.ListTopics(context.Background(), nil)

	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	if len(response.Topics) != 0 {
		t.Errorf("expected 0 topics, got %d", len(response.Topics))
	}
}

func TestGetMessages(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})

	s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Message 1",
	})
	s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Message 2",
	})

	response, err := s.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: topic.Id,
	})

	if err != nil {
		t.Fatalf("GetMessages failed: %v", err)
	}
	if len(response.Messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(response.Messages))
	}
}

func TestGetMessagesMultipleTopics(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic1, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})
	topic2, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "Random"})

	s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic1.Id,
		UserId:  user.Id,
		Text:    "Topic 1 Message",
	})
	s.PostMessage(context.Background(), &pb.PostMessageRequest{
		TopicId: topic2.Id,
		UserId:  user.Id,
		Text:    "Topic 2 Message",
	})

	response1, _ := s.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: topic1.Id,
	})
	response2, _ := s.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: topic2.Id,
	})

	if len(response1.Messages) != 1 {
		t.Errorf("expected 1 message in topic1, got %d", len(response1.Messages))
	}
	if len(response2.Messages) != 1 {
		t.Errorf("expected 1 message in topic2, got %d", len(response2.Messages))
	}
}

func TestGetSubscriptionNode(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})
	topic, _ := s.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: "General"})

	response, err := s.GetSubscriptionNode(context.Background(), &pb.SubscriptionNodeRequest{
		UserId:  user.Id,
		TopicId: []int64{topic.Id},
	})

	if err != nil {
		t.Fatalf("GetSubscriptionNode failed: %v", err)
	}
	if response.Node == nil {
		t.Fatal("expected node info to be set")
	}
	if response.Node.Address == "" {
		t.Error("expected node address to be set")
	}
	if response.SubscribeToken == "" {
		t.Error("expected subscribe token to be set")
	}
}

func TestGetSubscriptionNodeNoTopics(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})

	_, err := s.GetSubscriptionNode(context.Background(), &pb.SubscriptionNodeRequest{
		UserId:  user.Id,
		TopicId: []int64{},
	})

	if err == nil {
		t.Fatal("expected error for empty topic list")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument code, got %v", st.Code())
	}
}

func TestGetSubscriptionNodeTopicNotFound(t *testing.T) {
	s := NewMessageBoardServer()

	user, _ := s.CreateUser(context.Background(), &pb.CreateUserRequest{Name: "Alice"})

	_, err := s.GetSubscriptionNode(context.Background(), &pb.SubscriptionNodeRequest{
		UserId:  user.Id,
		TopicId: []int64{999},
	})

	if err == nil {
		t.Fatal("expected error for non-existent topic")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", st.Code())
	}
}
