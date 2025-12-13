package storage

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sort"
	"sync"
	"time"

	pb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrUserNotFound    = errors.New("User not found.")
	ErrTopicNotFound   = errors.New("Topic not found.")
	ErrMessageNotFound = errors.New("Message not found.")
	ErrTopicMismatch   = errors.New("Message topic mismatch.")
	ErrNotOwner        = errors.New("Not message owner.")
)

type subscriptionPermission struct {
	uporabnikID int64
	teme        map[int64]struct{}
	ustvarjeno  time.Time
}

type subscription struct {
	teme  map[int64]struct{}
	kanal chan *pb.MessageEvent
}

type State struct {
	mu sync.RWMutex

	nextUserID int64
	users      map[int64]*pb.User

	nextTopicID int64
	topics      map[int64]*pb.Topic

	nextMsgID int64
	msgs      map[int64]*pb.Message
	// TopicID -> []sporociloID (v vrstnem redu nastanka).
	msgIDsByTopic map[int64][]int64

	// Globalni event log (uporabi se za history pri SubscribeTopic).
	events []*pb.MessageEvent

	// Aktivne naročnine (vsaka naročnina ima EN kanal in množico tem).
	nextSubscribeID int64
	subscriptions   map[int64]*subscription

	// Globalno monotono naraščanje za MessageEvent.sequence_number.
	zaporednaStevilka int64

	// Deduplikacija likes: likedBy[topicID][messageID][userID] = true.
	// TODO: Dejva narediti map[int64]Sporocila + nek struct Sporocila, tuki je dost confusing.
	vsecMnozica map[int64]map[int64]map[int64]struct{}

	// Token -> dovoljenje.
	permissions map[string]*subscriptionPermission
}

func NewState() *State {
	return &State{
		nextUserID: 1,
		users:      make(map[int64]*pb.User),

		nextTopicID: 1,
		topics:      make(map[int64]*pb.Topic),

		nextMsgID:     1,
		msgs:          make(map[int64]*pb.Message),
		msgIDsByTopic: make(map[int64][]int64),

		events:        make([]*pb.MessageEvent, 0, 1024),
		subscriptions: make(map[int64]*subscription),

		zaporednaStevilka: 0,
		vsecMnozica:       make(map[int64]map[int64]map[int64]struct{}),
		permissions:       make(map[string]*subscriptionPermission),
	}
}

// Naročniki / publish-subscribe (EN kanal na naročnino + globalni event log).
//
// SubscribeTopicsWithHistory registrira naročnino in pod lockom naredi snapshot zgodovine.
// S tem preprečimo race: dogodek ne more "uide" med snapshotom in registracijo.
func (s *State) SubscribeTopicsWithHistory(topicIDs []int64, fromID int64) (narocninaID int64, kanal chan *pb.MessageEvent, zgodovina []*pb.MessageEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Registriraj naročnino.
	s.nextSubscribeID++
	nID := s.nextSubscribeID
	mnozicaTem := make(map[int64]struct{}, len(topicIDs))
	for _, id := range topicIDs {
		mnozicaTem[id] = struct{}{}
	}
	ch := make(chan *pb.MessageEvent, 4096)
	s.subscriptions[nID] = &subscription{teme: mnozicaTem, kanal: ch}

	// Snapshot zgodovine iz globalnega event loga.
	h := make([]*pb.MessageEvent, 0)
	for _, ev := range s.events {
		m := ev.GetMessage()
		if m == nil {
			continue
		}
		if _, ok := mnozicaTem[m.GetTopicId()]; !ok {
			continue
		}
		if fromID > 0 && m.GetId() < fromID {
			continue
		}

		// Subscriber dobi kopijo, da ne deli pointerjev na interno stanje.
		h = append(h, proto.Clone(ev).(*pb.MessageEvent))
	}

	return nID, ch, h
}

func (s *State) Unsubscribe(narocninaID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := s.subscriptions[narocninaID]
	if n == nil {
		return
	}
	delete(s.subscriptions, narocninaID)
	close(n.kanal)
}

func (s *State) notifySubscribersLocked(topicID int64, dogodek *pb.MessageEvent) {
	// OPOMBA: ta funkcija predpostavlja, da je klicatelj že pod s.zaklep.Lock().
	// Ne želimo blokirati celotnega strežnika zaradi počasnega odjemalca.
	// Če je subscriber kanal poln, subscriber odklopimo (zapremo kanal in ga odstranimo).

	if len(s.subscriptions) == 0 {
		return
	}

	zaOdstraniti := make([]int64, 0)
	for id, n := range s.subscriptions {
		if _, ok := n.teme[topicID]; !ok {
			continue
		}
		select {
		case n.kanal <- dogodek:
			// ok
		default:
			// subscriber je prepočasen -> konec streama (odjemalec dobi EOF)
			close(n.kanal)
			zaOdstraniti = append(zaOdstraniti, id)
		}
	}

	for _, id := range zaOdstraniti {
		delete(s.subscriptions, id)
	}
}

// Logiranje zaporednih številk in dogodkov v State.

func (s *State) createEventLocked(op pb.OpType, sporocilo *pb.Message, cas *timestamppb.Timestamp) *pb.MessageEvent {
	// OPOMBA: klicati samo pod s.zaklep.Lock().
	kopija := proto.Clone(sporocilo).(*pb.Message)
	if cas == nil {
		cas = timestamppb.Now()
	}

	s.zaporednaStevilka++
	ev := &pb.MessageEvent{
		SequenceNumber: s.zaporednaStevilka,
		Op:             op,
		Message:        kopija,
		EventAt:        cas,
	}

	// Globalni event log za history.
	s.events = append(s.events, ev)
	return ev
}

// Delo z userji.

func (s *State) GetUserID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextUserID
	s.nextUserID++
	return id
}

func (s *State) AddUser(uporabnik *pb.User) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.users[uporabnik.Id] = uporabnik
}

func (s *State) GetUser(uporabnikID int64) *pb.User {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.users[uporabnikID]
}

// Delo s topici.

func (s *State) GetTopicID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextTopicID
	s.nextTopicID++
	return id
}

func (s *State) AddTopic(topic *pb.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.topics[topic.Id] = topic
}

func (s *State) GetSingleTopic(topicID int64) *pb.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.topics[topicID]
}

func (s *State) GetAllTopics() []*pb.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]*pb.Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		res = append(res, topic)
	}

	// Sort za stabilen vrstni red (po ID).
	sort.Slice(res, func(i, j int) bool { return res[i].Id < res[j].Id })
	return res
}

// Delo s sporočili.

func (s *State) GetMsgID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextMsgID
	s.nextMsgID++
	return id
}

// CreateMessage ustvari sporočilo atomarno: preveri obstoj userja/topic, dodeli ID
// shrani sporočilo, zapiše dogodek v log in obvesti naročnike.
func (s *State) CreateMessage(topicID, userID int64, text string) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.users[userID] == nil {
		return nil, ErrUserNotFound
	}
	if s.topics[topicID] == nil {
		return nil, ErrTopicNotFound
	}

	id := s.nextMsgID
	s.nextMsgID++

	msg := &pb.Message{
		Id:        id,
		TopicId:   topicID,
		UserId:    userID,
		Text:      text,
		CreatedAt: timestamppb.Now(),
		Likes:     0,
	}

	cpy := proto.Clone(msg).(*pb.Message)
	s.msgs[cpy.Id] = cpy
	s.msgIDsByTopic[cpy.TopicId] = append(s.msgIDsByTopic[cpy.TopicId], cpy.Id)

	event := s.createEventLocked(pb.OpType_OP_POST, cpy, timestamppb.Now())
	s.notifySubscribersLocked(cpy.TopicId, event)

	return proto.Clone(cpy).(*pb.Message), nil
}

func (s *State) AddMessage(sporocilo *pb.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Vedno shranimo kopijo, da odjemalci ne dobijo pointerja na interno stanje.
	cpy := proto.Clone(sporocilo).(*pb.Message)
	s.msgs[cpy.Id] = cpy
	s.msgIDsByTopic[cpy.TopicId] = append(s.msgIDsByTopic[cpy.TopicId], cpy.Id)

	event := s.createEventLocked(pb.OpType_OP_POST, cpy, timestamppb.Now())
	s.notifySubscribersLocked(cpy.TopicId, event)
}

func (s *State) GetMessage(msgID int64) *pb.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	existing := s.msgs[msgID]
	if existing == nil {
		return nil
	}
	return proto.Clone(existing).(*pb.Message)
}

func (s *State) UpdateMessage(msg *pb.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.msgs[msg.Id]
	if existing != nil {
		// Ne prepiši likes/created_at itd. – posodobi samo tekst.
		existing.Text = msg.Text
		event := s.createEventLocked(pb.OpType_OP_UPDATE, existing, timestamppb.Now())
		s.notifySubscribersLocked(existing.TopicId, event)
		return
	}

	// Če sporočila ni bilo (robni primer), shrani kopijo.
	cpy := proto.Clone(msg).(*pb.Message)
	s.msgs[cpy.Id] = cpy
	event := s.createEventLocked(pb.OpType_OP_UPDATE, cpy, timestamppb.Now())
	s.notifySubscribersLocked(cpy.TopicId, event)
}

func (s *State) DeleteMessage(msgID int64, topicID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.msgs[msgID]
	delete(s.msgs, msgID)

	// Odstrani iz seznama po temi (linearno, OK za projekt).
	msgIDsByTopic := s.msgIDsByTopic[topicID]
	for i, currMsgID := range msgIDsByTopic {
		if currMsgID == msgID {
			// TODO: Niko dodaj kaj tukaj naredimo.
			s.msgIDsByTopic[topicID] = append(msgIDsByTopic[:i], msgIDsByTopic[i+1:]...)
			break
		}
	}

	if existing != nil {
		event := s.createEventLocked(pb.OpType_OP_DELETE, existing, timestamppb.Now())
		s.notifySubscribersLocked(topicID, event)
	}
}

func (s *State) LikeMessage(msgID int64) *pb.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.msgs[msgID]
	if existing == nil {
		return nil
	}

	existing.Likes++

	event := s.createEventLocked(pb.OpType_OP_LIKE, existing, timestamppb.Now())
	s.notifySubscribersLocked(existing.TopicId, event)

	return proto.Clone(existing).(*pb.Message)
}

// Atomarne operacije nad sporočili (thread-safe + brez data-race).

func (s *State) UpdateMessageText(topicID, userID, msgID int64, newText string) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := s.msgs[msgID]
	if m == nil {
		return nil, ErrMessageNotFound
	}
	if m.TopicId != topicID {
		return nil, ErrTopicMismatch
	}
	if m.UserId != userID {
		return nil, ErrNotOwner
	}

	m.Text = newText

	event := s.createEventLocked(pb.OpType_OP_UPDATE, m, timestamppb.Now())
	s.notifySubscribersLocked(topicID, event)

	return proto.Clone(m).(*pb.Message), nil
}

func (s *State) DeleteMessageIfOwner(topicID, userID, msgID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := s.msgs[msgID]
	if m == nil {
		return ErrMessageNotFound
	}
	if m.TopicId != topicID {
		return ErrTopicMismatch
	}
	if m.UserId != userID {
		return ErrNotOwner
	}

	delete(s.msgs, msgID)

	// Odstrani iz seznama po temi (linearno, OK za projekt).
	msgIDsByTopic := s.msgIDsByTopic[topicID]
	for i, id := range msgIDsByTopic {
		if id == msgID {
			s.msgIDsByTopic[topicID] = append(msgIDsByTopic[:i], msgIDsByTopic[i+1:]...)
			break
		}
	}

	event := s.createEventLocked(pb.OpType_OP_DELETE, m, timestamppb.Now())
	s.notifySubscribersLocked(topicID, event)
	return nil
}

func (s *State) LikeMessageInTopic(topicID, msgID, likerID int64) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := s.msgs[msgID]
	if m == nil {
		return nil, ErrMessageNotFound
	}
	if m.TopicId != topicID {
		return nil, ErrTopicMismatch
	}

	// Deduplikacija: en uporabnik lahko všečka sporočilo samo enkrat.
	if s.vsecMnozica[topicID] == nil {
		s.vsecMnozica[topicID] = make(map[int64]map[int64]struct{})
	}
	if s.vsecMnozica[topicID][msgID] == nil {
		s.vsecMnozica[topicID][msgID] = make(map[int64]struct{})
	}
	if _, ok := s.vsecMnozica[topicID][msgID][likerID]; ok {
		// Že všečkano -> vrni trenutno stanje brez spremembe.
		return proto.Clone(m).(*pb.Message), nil
	}
	s.vsecMnozica[topicID][msgID][likerID] = struct{}{}

	m.Likes++

	dogodek := s.createEventLocked(pb.OpType_OP_LIKE, m, timestamppb.Now())
	s.notifySubscribersLocked(topicID, dogodek)

	return proto.Clone(m).(*pb.Message), nil
}

// FromID je "starting id" (0 pomeni od začetka), limit 0 pomeni brez limita.
func (s *State) GetMessagesByTopic(topicID int64, fromID int64, limit int32) []*pb.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.msgIDsByTopic[topicID]
	res := make([]*pb.Message, 0)

	for _, id := range ids {
		if fromID > 0 && id < fromID {
			continue
		}
		sporocilo := s.msgs[id]
		if sporocilo == nil {
			continue
		}
		res = append(res, proto.Clone(sporocilo).(*pb.Message))

		if limit > 0 && int32(len(res)) >= limit {
			break
		}
	}

	return res
}

// Subscribe token (avtorizacija naročnin).
func (s *State) UstvariSubscribeToken(userID int64, topicIDs []int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	random_sl := make([]byte, 32)
	_, _ = rand.Read(random_sl)
	token := base64.RawURLEncoding.EncodeToString(random_sl)

	topicMap := make(map[int64]struct{}, len(topicIDs))
	for _, id := range topicIDs {
		topicMap[id] = struct{}{}
	}

	s.permissions[token] = &subscriptionPermission{
		uporabnikID: userID,
		teme:        topicMap,
		ustvarjeno:  time.Now(),
	}

	return token
}

func (s *State) PreveriSubscribeToken(token string, userID int64, topicIDs []int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	permission := s.permissions[token]
	if permission == nil {
		return false
	}
	if permission.uporabnikID != userID {
		return false
	}
	for _, id := range topicIDs {
		if _, ok := permission.teme[id]; !ok {
			return false
		}
	}
	return true
}
