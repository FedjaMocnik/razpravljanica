package storage

import (
	"sync"

	"github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
)

type State struct {
	mu      sync.Mutex
	userID  int64
	users   map[int64]*pb.User
	topicID int64
	topics  map[int64]*pb.Topic
}

func NewState() *State {
	return &State{
		userID:  1,
		users:   make(map[int64]*pb.User),
		topicID: 1,
		topics:  make(map[int64]*pb.Topic),
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
