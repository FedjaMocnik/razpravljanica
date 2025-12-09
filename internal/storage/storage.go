package storage

import (
	"sync"

	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
)

type State struct {
	mu          sync.Mutex
	userID      int64
	users       map[int64]*pb.User
	topicID     int64
	topics      map[int64]*pb.Topic
	msgID       int64
	messages    map[int64]*pb.Message
	subscribers map[int64][]chan *pb.MessageEvent
}

func NewState() *State {
	return &State{
		userID:      1,
		users:       make(map[int64]*pb.User),
		topicID:     1,
		topics:      make(map[int64]*pb.Topic),
		msgID:       1,
		messages:    make(map[int64]*pb.Message),
		subscribers: make(map[int64][]chan *pb.MessageEvent), // id -> inbox
	}
}

func (s *State) Subscribe(topicId int64) chan *pb.MessageEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan *pb.MessageEvent, 100)
	s.subscribers[topicId] = append(s.subscribers[topicId], ch)

	return ch
}

func (s *State) Unsubscribe(topicId int64, ch chan *pb.MessageEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	subs := s.subscribers[topicId]
	for i, sub := range subs {
		if sub == ch {
			s.subscribers[topicId] = append(subs[:i], subs[i+1:]...)
			close(ch)
			break
		}
	}
}

func (s *State) NotifySubscribers(topicId int64, event *pb.MessageEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ch := range s.subscribers[topicId] {
		select {
		case ch <- event:
		default:
		}
	}
}

func (s *State) GetUserID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.userID
	s.userID++
	return id
}

func (s *State) AddUser(u *pb.User) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.users[u.Id] = u
}

func (s *State) GetTopicID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.topicID
	s.topicID++
	return id
}

func (s *State) AddTopic(t *pb.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.topics[t.Id] = t
}

func (s *State) GetMsgID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.msgID
	s.msgID++
	return id
}

func (s *State) AddMessage(msg *pb.Message) {
	s.mu.Lock()
	s.messages[msg.Id] = msg
	s.mu.Unlock()

	s.NotifySubscribers(msg.TopicId, &pb.MessageEvent{
		Op:      pb.OpType_OP_POST,
		Message: msg,
	})
}

func (s *State) GetMessage(id int64) *pb.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.messages[id]
}

func (s *State) UpdateMessage(msg *pb.Message) {
	s.mu.Lock()
	s.messages[msg.Id] = msg
	s.mu.Unlock()

	s.NotifySubscribers(msg.TopicId, &pb.MessageEvent{
		Op:      pb.OpType_OP_UPDATE,
		Message: msg,
	})
}

func (s *State) DeleteMessage(id int64, topicId int64) {
	s.mu.Lock()
	msg := s.messages[id]
	delete(s.messages, id)
	s.mu.Unlock()

	if msg != nil {
		s.NotifySubscribers(topicId, &pb.MessageEvent{
			Op:      pb.OpType_OP_DELETE,
			Message: msg,
		})
	}
}

func (s *State) GetSingleTopic(id int64) *pb.Topic {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.topics[id]
}

func (s *State) GetAllTopics() []*pb.Topic {
	s.mu.Lock()
	defer s.mu.Unlock()

	// sl kot slice.
	slTopics := make([]*pb.Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		slTopics = append(slTopics, topic)
	}

	return slTopics
}

func (s *State) GetMessagesByTopic(topicId int64) []*pb.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	messages := make([]*pb.Message, 0)
	for _, msg := range s.messages {
		if msg.TopicId == topicId {
			messages = append(messages, msg)
		}
	}

	return messages
}
