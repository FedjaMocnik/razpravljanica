package storage

import (
	"testing"

	"github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestUpsertAndGetUser preveri osnovno shranjevanje in pridobivanje uporabnika.
func TestUpsertAndGetUser(t *testing.T) {
	s := NewState()
	user := &pb.User{Id: 10, Name: "Testko"}
	s.UpsertUser(user)

	got := s.GetUser(10)
	if got == nil {
		t.Fatalf("GetUser ni našel uporabnika")
	}
	if got.Name != "Testko" {
		t.Errorf("Pričakovano ime Testko, dobljeno %s", got.Name)
	}
}

// TestTopicUpdates preveri shranjevanje in posodabljanje teme.
func TestTopicUpdates(t *testing.T) {
	s := NewState()
	topic := &pb.Topic{Id: 5, Name: "Golang"}
	s.UpsertTopic(topic)

	got := s.GetTopic(5)
	if got == nil {
		t.Fatalf("GetTopic ni našel teme")
	}
	if got.Name != "Golang" {
		t.Errorf("Pričakovano ime Golang, dobljeno %s", got.Name)
	}
}

// TestMessageFlow preveri ustvarjanje in posodabljanje sporočil.
func TestMessageFlow(t *testing.T) {
	s := NewState()
	s.UpsertUser(&pb.User{Id: 1, Name: "A"})
	s.UpsertTopic(&pb.Topic{Id: 1, Name: "T"})

	// Priprava sporočila
	now := timestamppb.Now()
	msg, err := s.PreparePostMessage(1, 1, "Pozdravljen Svet", now)
	if err != nil {
		t.Fatalf("PreparePostMessage ni uspel: %v", err)
	}

	// Uveljavitev dogodka (simulacija replikacije)
	ev := &pb.MessageEvent{
		SequenceNumber: 1,
		Op:             pb.OpType_OP_POST,
		Message:        msg,
		EventAt:        now,
	}
	if err := s.ApplyReplicatedEvent(ev); err != nil {
		t.Fatalf("ApplyReplicatedEvent ni uspel: %v", err)
	}

	// Preveri, če je sporočilo v temi
	msgs := s.GetMessagesByTopic(1, 0, 100)
	if len(msgs) != 1 {
		t.Fatalf("Pričakovano 1 sporočilo, dobljeno %d", len(msgs))
	}
	if msgs[0].Text != "Pozdravljen Svet" {
		t.Errorf("Neskladje vsebine: %s", msgs[0].Text)
	}
}

// TestMessageUpdate preveri posodabljanje besedila sporočila.
func TestMessageUpdate(t *testing.T) {
	s := NewState()
	s.UpsertUser(&pb.User{Id: 1, Name: "A"})
	s.UpsertTopic(&pb.Topic{Id: 1, Name: "T"})

	now := timestamppb.Now()
	msg, _ := s.PreparePostMessage(1, 1, "Original", now)
	ev := &pb.MessageEvent{SequenceNumber: 1, Op: pb.OpType_OP_POST, Message: msg, EventAt: now}
	s.ApplyReplicatedEvent(ev)

	// Update
	updMsg, err := s.PrepareUpdateMessageText(1, 1, msg.Id, "Posodobljeno")
	if err != nil {
		t.Fatalf("PrepareUpdate ni uspel: %v", err)
	}
	evUpd := &pb.MessageEvent{SequenceNumber: 2, Op: pb.OpType_OP_UPDATE, Message: updMsg, EventAt: timestamppb.Now()}
	s.ApplyReplicatedEvent(evUpd)

	msgs := s.GetMessagesByTopic(1, 0, 100)
	if msgs[0].Text != "Posodobljeno" {
		t.Errorf("Sporočilo ni posodobljeno, dobljeno: %s", msgs[0].Text)
	}
}

// TestMissingDependencies preveri napake, če uporabnik ali tema ne obstajata.
func TestMissingDependencies(t *testing.T) {
	s := NewState()
	now := timestamppb.Now()
	_, err := s.PreparePostMessage(999, 999, "Napaka", now)
	if err == nil {
		t.Error("Pričakovana napaka, ker uporabnik/tema manjkata")
	}
}
