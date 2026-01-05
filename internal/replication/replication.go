// Package replication vsebuje "private" sloj za replikacijo med vozlišči.
//
// Implementira verižni model (chain replication):
//   - HEAD sprejme zapis, ga zapiše v lokalni log, posreduje naprej (Forward) in počaka na potrditev (Commit).
//   - Vmesna vozlišča Forward aplicirajo lokalno in posredujejo naslednjemu.
//   - TAIL po prejemu Forward entry smatra kot committed in pošlje Commit nazaj proti HEAD-u.
//
// Update RPC omogoča "catch-up"/bootstrap: vozlišče sporoči zadnji apliciran EntryId,
// predhodnik pa vrne manjkajoče log entry-je, ki se nato aplicirajo po vrsti.

package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/storage"
	privatepb "github.com/FedjaMocnik/razpravljalnica/pkgs/private/pb"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Manager je osrednji del verižne replikacije (chain replication) med vozlišči.
//
// Tok operacij:
//   1) HEAD: ustvari LogEntry (monoton EntryId), ga zapiše lokalno in pošlje naprej z RPC Forward.
//   2) Follower: entry aplicira na svoj state (v istem vrstnem redu) in ga posreduje naslednjemu.
//   3) TAIL: po prejemu Forward entry takoj šteje kot committed in sproži RPC Commit nazaj po verigi.
//   4) HEAD: ob prejemu Commit sprosti čakanje write operacije (waitForCommit).
//   TODO: Ne čakamo commita (implementacija verzioniranja).
//
// Update RPC je namenjen bootstrap/catch-up: predhodnik vrne vse manjkajoče vnose od lastKnownEntryId dalje.
//
// Opomba: po navodilih predpostavimo zanesljiva vozlišča (brez failover/re-konfiguracije),
// vendar Update še vedno uporabimo, da follower ob zagonu dohiti predhodnika.

type Manager struct {
	privatepb.UnimplementedReplicationServiceServer

	nodeID string // ID vozlišča (npr. "node1")
	addr   string // naslov (host:port), kjer posluša replication gRPC

	isHead bool // true, če je to HEAD verige (sprejema pisanja)
	isTail bool // true, če je to TAIL verige (servira branja, generira commite)

	prevAddr string // naslov prejšnjega vozlišča v verigi ("" => ni predhodnika)
	nextAddr string // naslov naslednjega vozlišča v verigi ("" => ni naslednika)

	state *storage.State // lokalni in-memory state; posodablja se ob applyEntry

	mu            sync.Mutex            // ščiti log/cursorje/waiterje in lazy gRPC povezave
	log           []*privatepb.LogEntry // zaporedni journal (vsi aplicirani entry-ji na tem node-u)
	lastApplied   uint64                // največji EntryId, ki je že apliciran lokalno
	lastCommitted uint64                // največji EntryId, za katerega smo prejeli commit
	nextEntryID   uint64                // generator EntryId; uporablja samo HEAD

	waiters map[uint64]chan struct{} // HEAD: kanali za čakanje na commit posameznega entry-ja

	// Pripravljenost (bootstrap): sledilna vozlišča niso "pripravljena", dokler ne dohitevajo svojega predhodnika.
	readyOnce sync.Once
	readyCh   chan struct{}

	// gRPC odjemalci (leno inicializirani)
	nextConn   *grpc.ClientConn
	nextClient privatepb.ReplicationServiceClient
	prevConn   *grpc.ClientConn
	prevClient privatepb.ReplicationServiceClient
}

type Config struct {
	NodeID   string
	Address  string
	PrevAddr string
	NextAddr string
	IsHead   bool
	IsTail   bool
}

func NewManager(cfg Config, st *storage.State) *Manager {
	m := &Manager{
		nodeID:   cfg.NodeID,
		addr:     cfg.Address,
		isHead:   cfg.IsHead,
		isTail:   cfg.IsTail,
		prevAddr: cfg.PrevAddr,
		nextAddr: cfg.NextAddr,
		state:    st,
		log:      make([]*privatepb.LogEntry, 0, 1024),
		waiters:  make(map[uint64]chan struct{}),
		readyCh:  make(chan struct{}),
	}
	// Glava (ali vozlišče brez predhodnika) je takoj pripravljeno.
	// TODO: Kaj če pride do tega, da sta dva takoj redi? Torej Glava in en, ki ponesreči dobi prev = ""?
	if cfg.IsHead || cfg.PrevAddr == "" {
		m.markReady()
	}
	return m
}

// IsReady vrne true, ko je vozlišče pripravljeno na delo (follower je najprej bootstrap-an).

func (m *Manager) IsReady() bool {
	select {
	case <-m.readyCh:
		return true
	default:
		return false
	}
}

func (m *Manager) markReady() {
	m.readyOnce.Do(func() { close(m.readyCh) })
}

// Close zapre odprte gRPC povezave do sosedov (če obstajajo).

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nextConn != nil {
		_ = m.nextConn.Close()
		m.nextConn, m.nextClient = nil, nil
	}
	if m.prevConn != nil {
		_ = m.prevConn.Close()
		m.prevConn, m.prevClient = nil, nil
	}
}

// dialNextLocked lazily odpre gRPC povezavo do naslednjega vozlišča v verigi.
// Klicatelj mora držati m.mu.

func (m *Manager) dialNextLocked() (privatepb.ReplicationServiceClient, error) {
	if m.nextAddr == "" {
		return nil, nil
	}
	if m.nextClient != nil {
		return m.nextClient, nil
	}
	cc, err := grpc.NewClient(m.nextAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	m.nextConn = cc
	m.nextClient = privatepb.NewReplicationServiceClient(cc)
	return m.nextClient, nil
}

// dialPrevLocked leno odpre gRPC povezavo do prejšnjega vozlišča v verigi.
// Klicatelj mora držati m.mu.

func (m *Manager) dialPrevLocked() (privatepb.ReplicationServiceClient, error) {
	if m.prevAddr == "" {
		return nil, nil
	}
	if m.prevClient != nil {
		return m.prevClient, nil
	}
	cc, err := grpc.NewClient(m.prevAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	m.prevConn = cc
	m.prevClient = privatepb.NewReplicationServiceClient(cc)
	return m.prevClient, nil
}

// BootstrapFromPrev pridobi in aplicira celoten log z vozlišča predhodnika.
// To je uporabno, ko zaganjamo sledilno vozlišče, ki začne s praznim stanjem.
func (m *Manager) BootstrapFromPrev(ctx context.Context) error {
	m.mu.Lock()
	prevAddr := m.prevAddr
	m.mu.Unlock()
	if prevAddr == "" {
		return nil
	}

	// Ob zagonu poskusimo večkrat.
	for i := 0; i < 20; i++ {
		if err := m.catchUpOnce(ctx); err == nil {
			m.markReady()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("bootstrap ni uspel (prev=%s)", prevAddr)
}

// catchUpOnce zahteva od predhodnika manjkajoče entry-je (Update) in jih aplicira zaporedno.

func (m *Manager) catchUpOnce(ctx context.Context) error {
	m.mu.Lock()
	last := m.lastApplied
	client, err := m.dialPrevLocked()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if client == nil {
		return nil
	}
	resp, err := client.Update(ctx, &privatepb.UpdateRequest{LastKnownEntryId: last})
	if err != nil {
		return err
	}
	for _, e := range resp.GetMissingEntries() {
		if err := m.applyEntry(ctx, e, true); err != nil {
			return err
		}
	}
	return nil
}

func encodePayload(msg proto.Message) ([]byte, error) {
	a, err := anypb.New(msg)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(a)
}

func decodePayload(b []byte) (proto.Message, error) {
	var a anypb.Any
	if err := proto.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	// Pričakujemo le majhen, fiksen nabor tipov.
	// Najprej poskusimo MessageEvent, nato User, nato Topic.
	var ev publicpb.MessageEvent
	if a.MessageIs(&ev) {
		if err := a.UnmarshalTo(&ev); err != nil {
			return nil, err
		}
		return &ev, nil
	}
	var u publicpb.User
	if a.MessageIs(&u) {
		if err := a.UnmarshalTo(&u); err != nil {
			return nil, err
		}
		return &u, nil
	}
	var t publicpb.Topic
	if a.MessageIs(&t) {
		if err := a.UnmarshalTo(&t); err != nil {
			return nil, err
		}
		return &t, nil
	}
	return nil, fmt.Errorf("nepodprta payload vrsta: %s", a.TypeUrl)
}

// Lokalno dodamo in apliciramo entry. Če je forward==true, ga posredujemo tudi naslednjemu (uporabno le pri replay-u iz Update).
func (m *Manager) applyEntry(ctx context.Context, entry *privatepb.LogEntry, forward bool) error {
	if entry == nil {
		return fmt.Errorf("nil entry")
	}

	m.mu.Lock()
	if entry.EntryId <= m.lastApplied {
		// že aplicirano
		m.mu.Unlock()
		return nil
	}
	if entry.EntryId != m.lastApplied+1 {
		// Napačen vrstni red. V tem projektu zahtevamo strogo zaporedje.
		want := m.lastApplied + 1
		m.mu.Unlock()
		return fmt.Errorf("out-of-order entry (imam %d, dobil %d)", want, entry.EntryId)
	}
	// dodaj v log
	// Pomembno: entry kloniramo (proto.Clone), da se ne zanašamo na življenjsko dobo req objekta.
	m.log = append(m.log, proto.Clone(entry).(*privatepb.LogEntry))
	m.lastApplied = entry.EntryId
	m.mu.Unlock()

	// Spremembe apliciramo na state izven manager locka.
	payloadMsg, err := decodePayload(entry.Payload)
	if err != nil {
		return err
	}
	switch msg := payloadMsg.(type) {
	// Payload je Any, ki lahko vsebuje User/Topic/MessageEvent.
	// MessageEvent predstavlja operacijo (POST/LIKE/UPDATE/DELETE) in mora ohraniti točen vrstni red.
	case *publicpb.User:
		m.state.UpsertUser(msg)
	case *publicpb.Topic:
		m.state.UpsertTopic(msg)
	case *publicpb.MessageEvent:
		if err := m.state.ApplyReplicatedEvent(msg); err != nil {
			return err
		}
	default:
		return fmt.Errorf("neznan payload")
	}

	if forward {
		// Po najboljših močeh: ponovno posreduj naprej po verigi.
		m.mu.Lock()
		client, err := m.dialNextLocked()
		m.mu.Unlock()
		if err != nil {
			return err
		}
		if client != nil {
			_, err := client.Forward(ctx, &privatepb.ForwardRequest{Entry: entry})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//gRPC: ReplicationService

func (m *Manager) Forward(ctx context.Context, req *privatepb.ForwardRequest) (*privatepb.CommitRequest, error) {
	entry := req.GetEntry()
	if entry == nil {
		return nil, fmt.Errorf("manjka entry")
	}
	if err := m.applyEntry(ctx, entry, false); err != nil {
		// Idempotentnost + strict ordering se uveljavlja v applyEntry (EntryId mora priti zaporedno).
		return nil, err
	}

	// Posreduj naslednjemu, če obstaja.
	m.mu.Lock()
	isTail := m.isTail
	client, err := m.dialNextLocked()
	prevClient, prevErr := m.dialPrevLocked()
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if prevErr != nil {
		return nil, prevErr
	}

	var committed uint64
	if client != nil {
		resp, err := client.Forward(ctx, req)
		if err != nil {
			return nil, err
		}
		committed = resp.GetCommittedEntryId()
	}

	if isTail {
		// Na repu verige se entry smatra kot committed takoj, ko je apliciran na tail-u.
		// Nato se Commit sinhrono pošlje nazaj prejšnjemu node-u (da ohranimo ordering commitov).
		committed = entry.EntryId
		// Na repu verige commitamo takoj in commit posredujemo nazaj po verigi.
		m.mu.Lock()
		if committed > m.lastCommitted {
			m.lastCommitted = committed
		}
		m.mu.Unlock()
		if prevClient != nil {
			// sinhron commit za ohranitev vrstnega reda
			_, _ = prevClient.Commit(ctx, &privatepb.CommitRequest{CommittedEntryId: committed})
		}
	}

	// Po najboljših močeh: vrni trenutno committed vrednost.
	m.mu.Lock()
	if m.lastCommitted > committed {
		committed = m.lastCommitted
	}
	m.mu.Unlock()
	return &privatepb.CommitRequest{CommittedEntryId: committed}, nil
}

// Commit je "control" RPC, ki potuje od TAIL-a proti HEAD-u in potrjuje, da je entry committed.
// HEAD ob tem sprosti gorutine, ki čakajo v waitForCommit.

func (m *Manager) Commit(ctx context.Context, req *privatepb.CommitRequest) (*emptypb.Empty, error) {
	commitID := req.GetCommittedEntryId()

	// Posodobi committed stanje in sproži čakajoče (waiterje).
	// Commit je lahko prejet večkrat (npr. zaradi retryjev); zato lastCommitted posodobimo monotono.
	var toSignal []chan struct{}
	m.mu.Lock()
	if commitID > m.lastCommitted {
		m.lastCommitted = commitID
	}
	if m.isHead {
		for id, ch := range m.waiters {
			if id <= commitID {
				toSignal = append(toSignal, ch)
				delete(m.waiters, id)
			}
		}
	}
	prevClient, err := m.dialPrevLocked()
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	for _, ch := range toSignal {
		close(ch)
	}

	// Posreduj nazaj po verigi.
	if prevClient != nil {
		_, _ = prevClient.Commit(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

// Update vrne manjkajoče LogEntry-je od (LastKnownEntryId) naprej.
// Uporablja se za bootstrap/catch-up followerjev.

func (m *Manager) Update(ctx context.Context, req *privatepb.UpdateRequest) (*privatepb.UpdateResponse, error) {
	last := req.GetLastKnownEntryId()
	m.mu.Lock()
	defer m.mu.Unlock()

	if last >= m.lastApplied {
		// Krajšnica: sledilec trdi, da ima vse.
		return &privatepb.UpdateResponse{MissingEntries: nil}, nil
	}
	// entry_id je logično 1-indeksiran, v slice-u pa je shranjen 0-indeksirano.
	// TODO: 0 indeksiraj entry_id.

	// Zagotovimo, da je start \in [0, len(m.log)].
	start := max(0, int(last))
	start = min(len(m.log), start)

	out := make([]*privatepb.LogEntry, 0, len(m.log)-start)
	for i := start; i < len(m.log); i++ {
		out = append(out, proto.Clone(m.log[i]).(*privatepb.LogEntry))
	}
	return &privatepb.UpdateResponse{MissingEntries: out}, nil
}

//Pomožne funkcije na strani HEAD-a (uporablja public API)

func (m *Manager) ensureHead() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isHead {
		return fmt.Errorf("ta node ni HEAD")
	}
	return nil
}

func (m *Manager) nextLogEntryIDLocked() uint64 {
	m.nextEntryID++
	return m.nextEntryID
}

func (m *Manager) appendLocalEntryLocked(e *privatepb.LogEntry) {
	m.log = append(m.log, proto.Clone(e).(*privatepb.LogEntry))
	m.lastApplied = e.EntryId
}

// forwardToNext pošlje entry naslednjemu node-u; če veriga nima naslednika, gre za commit v enovozliščni postavitvi.

func (m *Manager) forwardToNext(ctx context.Context, e *privatepb.LogEntry) error {
	m.mu.Lock()
	client, err := m.dialNextLocked()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if client == nil {
		// enovozliščna postavitev (samo eno vozlišče)
		// commitaj lokalno
		_, _ = m.Commit(ctx, &privatepb.CommitRequest{CommittedEntryId: e.EntryId})
		return nil
	}
	_, err = client.Forward(ctx, &privatepb.ForwardRequest{Entry: e})
	return err
}

// waitForCommit blokira (ali do ctx cancel), dokler ne prejmemo Commit za entryID.

func (m *Manager) waitForCommit(ctx context.Context, entryID uint64) error {
	m.mu.Lock()
	if m.lastCommitted >= entryID {
		m.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	// Kanal se zapre v Commit(), ko lastCommitted doseže entryID (zapiranje deluje kot "broadcast" signal).
	m.waiters[entryID] = ch
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		// Če je caller obupal, počistimo waiter (da ne puščamo rasti map-e).
		m.mu.Lock()
		if w, ok := m.waiters[entryID]; ok && w == ch {
			delete(m.waiters, entryID)
		}
		m.mu.Unlock()

		return ctx.Err()
	case <-ch:
		return nil
	}
}

// ReplicateMessageEvent doda MessageEvent v replikacijski log in počaka na commit.
func (m *Manager) ReplicateMessageEvent(ctx context.Context, ev *publicpb.MessageEvent) error {
	if ev == nil {
		return fmt.Errorf("nil event")
	}
	if err := m.ensureHead(); err != nil {
		return err
	}

	payload, err := encodePayload(ev)
	if err != nil {
		return err
	}

	m.mu.Lock()
	id := m.nextLogEntryIDLocked()
	entry := &privatepb.LogEntry{EntryId: id, Timestamp: timestamppb.Now(), Payload: payload}
	m.appendLocalEntryLocked(entry)
	m.mu.Unlock()

	if err := m.forwardToNext(ctx, entry); err != nil {
		return err
	}
	return m.waitForCommit(ctx, id)
}

// ReplicateUser replicira ustvarjenega/posodobljenega uporabnika na vsa vozlišča (samo na HEAD-u).

func (m *Manager) ReplicateUser(ctx context.Context, u *publicpb.User) error {
	if u == nil {
		return fmt.Errorf("nil user")
	}
	if err := m.ensureHead(); err != nil {
		return err
	}
	payload, err := encodePayload(u)
	if err != nil {
		return err
	}
	m.mu.Lock()
	id := m.nextLogEntryIDLocked()
	entry := &privatepb.LogEntry{EntryId: id, Timestamp: timestamppb.Now(), Payload: payload}
	m.appendLocalEntryLocked(entry)
	m.mu.Unlock()

	if err := m.forwardToNext(ctx, entry); err != nil {
		return err
	}
	return m.waitForCommit(ctx, id)
}

// ReplicateTopic replicira ustvarjeno temo na vsa vozlišča (samo na HEAD-u).

func (m *Manager) ReplicateTopic(ctx context.Context, t *publicpb.Topic) error {
	if t == nil {
		return fmt.Errorf("nil topic")
	}
	if err := m.ensureHead(); err != nil {
		return err
	}
	payload, err := encodePayload(t)
	if err != nil {
		return err
	}
	m.mu.Lock()
	id := m.nextLogEntryIDLocked()
	entry := &privatepb.LogEntry{EntryId: id, Timestamp: timestamppb.Now(), Payload: payload}
	m.appendLocalEntryLocked(entry)
	m.mu.Unlock()

	if err := m.forwardToNext(ctx, entry); err != nil {
		return err
	}
	return m.waitForCommit(ctx, id)
}
