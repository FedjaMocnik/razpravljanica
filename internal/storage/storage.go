// Paket storage implementira "in-memory" shrambo stanja razpravljalnice.
//
// State je zaščiten z RWMutex, zato je varen za hkratni dostop več odjemalcev.
// Poleg osnovnih podatkov (uporabniki, teme, sporočila) hrani tudi globalni event log,
// ki se uporablja za zgodovino pri SubscribeTopic.
//
// Pri verižni replikaciji glava generira MessageEvent-e z določenim sequence_number,
// sledilna vozlišča pa jih aplicirajo z ApplyReplicatedEvent, da vsa vozlišča vidijo
// enak vrstni red dogodkov.

package storage

import (
	"errors"
	"sort"
	"sync"

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

type subscription struct {
	teme  map[int64]struct{}
	kanal chan *pb.MessageEvent
}

type State struct {
	mu sync.RWMutex
	// mu ščiti vso interno stanje (RWMutex: več bralcev ali en pisec).
	// Vse metode v tem paketu so zasnovane tako, da ne vračajo pointerjev na interno stanje,
	// temveč vrnejo kopije (proto.Clone), da se izognemo nenamernim spremembam odjemalca.

	// Števci za generiranje novih ID-jev. Na glavi se povečujejo pri ustvarjanju,
	// na drugih vozliščih pa se lahko premaknejo naprej pri replikaciji, da ne pride do ponovne uporabe.

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

	// Deduplikacija všečkov: vsecMnozica[topicID][messageID][userID] vsebuje uporabnike, ki so že všečkali.
	// TODO: Dejva narediti map[int64]Sporocila + nek struct Sporocila, tuki je dost confusing.
	vsecMnozica map[int64]map[int64]map[int64]struct{}
}

// NextSequenceNumber rezervira naslednjo globalno zaporedno številko za MessageEvent.
func (s *State) NextSequenceNumber() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.zaporednaStevilka++
	return s.zaporednaStevilka
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
	}
}

// UpsertUser vstavi uporabnika z že določenim ID-jem (uporabno na repliki/followerju).
// Hkrati posodobi tudi interni števec nextUserID, da ne pride do kolizij pri novih ID-jih.
func (s *State) UpsertUser(uporabnik *pb.User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if uporabnik == nil {
		return
	}
	cpy := proto.Clone(uporabnik).(*pb.User)
	s.users[cpy.Id] = cpy
	if cpy.Id >= s.nextUserID {
		s.nextUserID = cpy.Id + 1
	}
}

// UpsertTopic vstavi temo z že določenim ID-jem (uporabno na repliki/followerju).
// Hkrati posodobi tudi interni števec nextTopicID, da ne pride do kolizij pri novih ID-jih.
func (s *State) UpsertTopic(topic *pb.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if topic == nil {
		return
	}
	cpy := proto.Clone(topic).(*pb.Topic)
	s.topics[cpy.Id] = cpy
	if cpy.Id >= s.nextTopicID {
		s.nextTopicID = cpy.Id + 1
	}
}

// ApplyReplicatedEvent aplicira replikiran MessageEvent s fiksno sequence_number.
// To uporabljamo pri verižni replikaciji, da vsa vozlišča vidijo enak vrstni red dogodkov.
func (s *State) ApplyReplicatedEvent(ev *pb.MessageEvent) error {
	if ev == nil || ev.Message == nil {
		return errors.New("neveljaven event")
	}
	// Kopiramo takoj, da ne držimo referenc na zunanje (tuje) pointerje.
	evCpy := proto.Clone(ev).(*pb.MessageEvent)
	msg := evCpy.Message

	s.mu.Lock()
	defer s.mu.Unlock()

	// Poskrbi, da je števec zaporednih številk monotono naraščajoč.
	if evCpy.SequenceNumber > s.zaporednaStevilka {
		s.zaporednaStevilka = evCpy.SequenceNumber
	} else if evCpy.SequenceNumber == 0 {
		// Varnost: če je klicatelj pozabil nastaviti seq, dodeli monotono naraščajočo.
		s.zaporednaStevilka++
		evCpy.SequenceNumber = s.zaporednaStevilka
	}

	switch evCpy.Op {
	case pb.OpType_OP_POST:
		// Poskrbi, da je nextMsgID dovolj velik (da se novi ID-ji ne ponavljajo).
		if msg.Id >= s.nextMsgID {
			s.nextMsgID = msg.Id + 1
		}
		// Vstavi sporočilo (približno idempotentno – če že obstaja, ga ne podvajamo).
		if s.msgs[msg.Id] == nil {
			s.msgs[msg.Id] = proto.Clone(msg).(*pb.Message)
			s.msgIDsByTopic[msg.TopicId] = append(s.msgIDsByTopic[msg.TopicId], msg.Id)
		}
	case pb.OpType_OP_UPDATE:
		existing := s.msgs[msg.Id]
		if existing == nil {
			// Če sporočilo manjka, shrani celoten posnetek.
			if msg.Id >= s.nextMsgID {
				s.nextMsgID = msg.Id + 1
			}
			s.msgs[msg.Id] = proto.Clone(msg).(*pb.Message)
		} else {
			existing.Text = msg.Text
			existing.Likes = msg.Likes
		}
	case pb.OpType_OP_DELETE:
		// Briši po ID-ju.
		delete(s.msgs, msg.Id)
		ids := s.msgIDsByTopic[msg.TopicId]
		for i, id := range ids {
			if id == msg.Id {
				s.msgIDsByTopic[msg.TopicId] = append(ids[:i], ids[i+1:]...)
				break
			}
		}
	case pb.OpType_OP_LIKE:
		existing := s.msgs[msg.Id]
		if existing == nil {
			if msg.Id >= s.nextMsgID {
				s.nextMsgID = msg.Id + 1
			}
			s.msgs[msg.Id] = proto.Clone(msg).(*pb.Message)
		} else {
			existing.Likes = msg.Likes
		}
	default:
		return errors.New("nepodprta op")
	}

	// Dodaj v globalni event log in obvesti naročnike.
	s.events = append(s.events, evCpy)
	s.notifySubscribersLocked(msg.TopicId, evCpy)
	return nil
}

// Naročniki / publish-subscribe (EN kanal na naročnino + globalni event log).
//
// SubscribeTopicsWithHistory registrira naročnino in pod zaklepom naredi posnetek zgodovine.
// S tem preprečimo pogoje tekmovanja: dogodek ne more uideti med posnetkom in registracijo.
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

// PreparePostMessage preveri, da uporabnik in tema obstajata, ter rezervira nov
// ID sporočila, vendar sporočila NE shrani v stanje in NE
// obvesti naročnikov.
//
// Vrnjeno Message je namenjeno, da ga oviješ v replikacijski MessageEvent
// (OP_POST) in ga kasneje apliciraš prek ApplyReplicatedEvent.
func (s *State) PreparePostMessage(topicID, userID int64, text string, createdAt *timestamppb.Timestamp) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.users[userID] == nil {
		return nil, ErrUserNotFound
	}
	if s.topics[topicID] == nil {
		return nil, ErrTopicNotFound
	}
	if createdAt == nil {
		createdAt = timestamppb.Now()
	}

	id := s.nextMsgID
	s.nextMsgID++ // reserve ID (head-only writer)

	msg := &pb.Message{Id: id, TopicId: topicID, UserId: userID, Text: text, CreatedAt: createdAt, Likes: 0}
	return proto.Clone(msg).(*pb.Message), nil
}

// PrepareUpdateMessageText preveri lastništvo in pripadnost temi ter vrne
// posodobljen posnetek sporočila, vendar NE spremeni shranjenega sporočila.
func (s *State) PrepareUpdateMessageText(topicID, userID, msgID int64, newText string) (*pb.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

	cpy := proto.Clone(m).(*pb.Message)
	cpy.Text = newText
	return cpy, nil
}

// PrepareDeleteMessageIfOwner preveri lastništvo in vrne posnetek sporočila,
// ki bi se izbrisalo, vendar ga NE izbriše iz stanja.
func (s *State) PrepareDeleteMessageIfOwner(topicID, userID, msgID int64) (*pb.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	return proto.Clone(m).(*pb.Message), nil
}

// PrepareLikeMessage preveri, ali je uporabnik že všečkal
// sporočilo (deduplikacija na glavi) in vrne posodobljen posnetek sporočila,
// vendar množice všečkov NE spremeni in NE posodobi likes v stanju.
//
// Bool povratek pove, ali bi všeček dejansko spremenil stanje (ni duplikat).
func (s *State) PrepareLikeMessage(topicID, msgID, likerID int64) (*pb.Message, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m := s.msgs[msgID]
	if m == nil {
		return nil, false, ErrMessageNotFound
	}
	if m.TopicId != topicID {
		return nil, false, ErrTopicMismatch
	}

	// Preveri deduplikacijsko mapo brez spreminjanja stanja.
	if byTopic, ok := s.vsecMnozica[topicID]; ok {
		if byMsg, ok := byTopic[msgID]; ok {
			if _, ok := byMsg[likerID]; ok {
				return proto.Clone(m).(*pb.Message), false, nil
			}
		}
	}

	cpy := proto.Clone(m).(*pb.Message)
	cpy.Likes = m.Likes + 1
	return cpy, true, nil
}

// MarkLike zabeleži, da je likerID všečkal (topicID,msgID). To glava uporabi
// po tem, ko je všeček commit-an in apliciran prek ApplyReplicatedEvent.
func (s *State) MarkLike(topicID, msgID, likerID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.vsecMnozica[topicID] == nil {
		s.vsecMnozica[topicID] = make(map[int64]map[int64]struct{})
	}
	if s.vsecMnozica[topicID][msgID] == nil {
		s.vsecMnozica[topicID][msgID] = make(map[int64]struct{})
	}
	s.vsecMnozica[topicID][msgID][likerID] = struct{}{}
}

// CreateMessageNoEvent je podobna CreateMessage, vendar NE generira MessageEvent
// in NE obvesti naročnikov. Glava to uporabi, da lahko
// replicira vnaprej določen MessageEvent (z določeno sequence_number) na followerje.
func (s *State) CreateMessageNoEvent(topicID, userID int64, text string, createdAt *timestamppb.Timestamp) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.users[userID] == nil {
		return nil, ErrUserNotFound
	}
	if s.topics[topicID] == nil {
		return nil, ErrTopicNotFound
	}

	if createdAt == nil {
		createdAt = timestamppb.Now()
	}

	id := s.nextMsgID
	s.nextMsgID++

	msg := &pb.Message{Id: id, TopicId: topicID, UserId: userID, Text: text, CreatedAt: createdAt, Likes: 0}
	cpy := proto.Clone(msg).(*pb.Message)
	s.msgs[cpy.Id] = cpy
	s.msgIDsByTopic[cpy.TopicId] = append(s.msgIDsByTopic[cpy.TopicId], cpy.Id)
	return proto.Clone(cpy).(*pb.Message), nil
}

// UpdateMessageTextNoEvent posodobi besedilo, vendar NE generira dogodka.
func (s *State) UpdateMessageTextNoEvent(topicID, userID, msgID int64, newText string) (*pb.Message, error) {
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
	return proto.Clone(m).(*pb.Message), nil
}

// DeleteMessageIfOwnerNoEvent izbriše sporočilo, vendar NE generira dogodka.
// Vrne posnetek izbrisanega sporočila.
func (s *State) DeleteMessageIfOwnerNoEvent(topicID, userID, msgID int64) (*pb.Message, error) {
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
	snapshot := proto.Clone(m).(*pb.Message)
	delete(s.msgs, msgID)

	ids := s.msgIDsByTopic[topicID]
	for i, id := range ids {
		if id == msgID {
			s.msgIDsByTopic[topicID] = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	return snapshot, nil
}

// LikeMessageInTopicNoEvent uporabi deduplikacijo všečkov, vendar NE generira dogodka.
// Vrne posodobljen posnetek sporočila in bool, ki pove, ali je všeček
// dejansko spremenil stanje (torej ni bil duplikat).
func (s *State) LikeMessageInTopicNoEvent(topicID, msgID, likerID int64) (*pb.Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := s.msgs[msgID]
	if m == nil {
		return nil, false, ErrMessageNotFound
	}
	if m.TopicId != topicID {
		return nil, false, ErrTopicMismatch
	}

	if s.vsecMnozica[topicID] == nil {
		s.vsecMnozica[topicID] = make(map[int64]map[int64]struct{})
	}
	if s.vsecMnozica[topicID][msgID] == nil {
		s.vsecMnozica[topicID][msgID] = make(map[int64]struct{})
	}
	if _, ok := s.vsecMnozica[topicID][msgID][likerID]; ok {
		// Duplikat všečka: stanje se ne spremeni.
		return proto.Clone(m).(*pb.Message), false, nil
	}
	s.vsecMnozica[topicID][msgID][likerID] = struct{}{}
	m.Likes++
	return proto.Clone(m).(*pb.Message), true, nil
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

// Atomarne operacije nad sporočili (varno za več niti + brez pogojev tekmovanja).

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

// FromID je "začetni ID" (0 pomeni od začetka), limit 0 pomeni brez omejitve.
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
