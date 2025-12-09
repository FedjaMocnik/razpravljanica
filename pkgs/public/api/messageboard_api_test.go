package api

import (
	"context"
	"testing"

	"github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
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
