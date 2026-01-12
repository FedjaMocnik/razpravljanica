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
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/storage"
	privatepb "github.com/FedjaMocnik/razpravljalnica/pkgs/private/pb"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	structpb "google.golang.org/protobuf/types/known/structpb"
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

	waiters map[uint64]chan error // HEAD: kanali za čakanje na commit posameznega entry-ja

	// Pripravljenost (bootstrap): sledilna vozlišča niso "pripravljena", dokler ne dohitevajo svojega predhodnika.
	readyOnce sync.Once
	readyCh   chan struct{}

	// Dinamična re-konfiguracija: ko se veriga spremeni (npr. odpove vmesni node),
	// lahko sosedi potrebujejo "catch-up". Med catch-up fazo lahko začasno
	// blokiramo naročnine/branja na tem node-u.
	syncing uint32 // 0 = ne, 1 = da

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

// ErrWriteAborted se vrne pisalnim operacijam, ki čakajo na commit,
// kadar pride do re-konfiguracije verige in se ne-commitani vnosi zavržejo.
var ErrWriteAborted = errors.New("write aborted: topology changed before commit")

func signalWaiter(ch chan error, err error) {
	if ch == nil {
		return
	}
	// Kanal je bufferiran, da se izognemo deadlocku, če je receiver ravno odpovedal.
	select {
	case ch <- err:
	default:
	}
	close(ch)
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
		waiters:  make(map[uint64]chan error),
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
	if atomic.LoadUint32(&m.syncing) == 1 {
		return false
	}
	select {
	case <-m.readyCh:
		return true
	default:
		return false
	}
}

// UpdateTopology spremeni soseda (prev/next) in vlogo (head/tail) med delovanjem.
// Vrne true, če se je previous naslov spremenil (kar običajno pomeni, da je potreben catch-up).
func (m *Manager) UpdateTopology(prevAddr, nextAddr string, isHead, isTail bool) (prevChanged bool) {
	var (
		doRebuild    bool
		replayLog    []*privatepb.LogEntry
		committedID  uint64
		becameHead   bool
		becameTail   bool
		abortWaiters []chan error
	)

	m.mu.Lock()

	oldIsHead := m.isHead
	oldIsTail := m.isTail
	oldNextAddr := m.nextAddr
	becameHead = isHead && !oldIsHead
	becameTail = isTail && !oldIsTail
	nextChanged := false

	if m.prevAddr != prevAddr {
		prevChanged = true
		m.prevAddr = prevAddr
		if m.prevConn != nil {
			_ = m.prevConn.Close()
			m.prevConn, m.prevClient = nil, nil
		}
	}
	if m.nextAddr != nextAddr {
		nextChanged = true
		m.nextAddr = nextAddr
		if m.nextConn != nil {
			_ = m.nextConn.Close()
			m.nextConn, m.nextClient = nil, nil
		}
	}

	// Poseben primer: če smo HEAD in ostanemo HEAD, a se spremeni "next" (npr. odpove sredinski node
	// in veriga se preveže), lahko obstajajo ne-commitani vnosi, ki nikoli ne bodo commitani.
	// Takšne vnose moramo zavreči (truncate) in pisalce, ki čakajo na commit, obvestiti z napako.
	headNextChanged := oldIsHead && isHead && (oldNextAddr != nextAddr) && nextChanged

	// Če se spremenijo vloge (postanemo HEAD ali TAIL), moramo biti commit-safe:
	// - nikoli ne smemo "commitati" ne-commit-anih entry-jev
	// - ob preklopu lahko obstajajo aplicirani, a še ne-commitani vnosi (lastApplied > lastCommitted),
	//   ki jih je treba rollbackati (truncate) in ponovno zgraditi stanje iz commit-anega loga.
	if becameHead || becameTail || headNextChanged {
		if m.lastApplied > m.lastCommitted {
			// log slice je 1:1 z EntryId (EntryId=1 je index 0), zato je dolžina commit-anega dela == lastCommitted.
			if int(m.lastCommitted) < len(m.log) {
				m.log = m.log[:int(m.lastCommitted)]
			}
			m.lastApplied = m.lastCommitted
		}
		// Če postanemo head (ali ostanemo head, a se next spremeni), naj generator nadaljuje od zadnjega COMMIT-anega EntryId.
		if becameHead || headNextChanged {
			m.nextEntryID = m.lastCommitted
		}
		// Ob preklopu v HEAD resetiramo waiterje (na followerju jih ne bi smelo biti).
		if becameHead {
			m.waiters = make(map[uint64]chan error)
		}
		// Ob re-konfiguraciji HEAD-a ob spremembi next: abortiramo vse waiterje za ne-commitane entry-je.
		if headNextChanged {
			for id, ch := range m.waiters {
				if id > m.lastCommitted {
					abortWaiters = append(abortWaiters, ch)
					delete(m.waiters, id)
				}
			}
		}
		committedID = m.lastCommitted

		// Snapshot commit-anega loga za replay (zunaj m.mu).
		replayLog = make([]*privatepb.LogEntry, len(m.log))
		for i := range m.log {
			replayLog[i] = proto.Clone(m.log[i]).(*privatepb.LogEntry)
		}
		doRebuild = true
	}

	if isHead || prevAddr == "" {
		m.markReady()
	}
	m.isHead = isHead
	m.isTail = isTail
	m.mu.Unlock()

	for _, ch := range abortWaiters {
		signalWaiter(ch, ErrWriteAborted)
	}

	if doRebuild {
		if err := m.rebuildStateFromLog(replayLog, committedID); err != nil {
			log.Printf("node %s: rebuild state failed (committed=%d): %v", m.nodeID, committedID, err)
		}
	}
	return prevChanged
}

// rebuildStateFromLog popolnoma resetira lokalni State in ga ponovno zgradi iz commit-anega dela loga.
// Uporabimo ga pri re-konfiguraciji (npr. ko node postane nov HEAD/TAIL), da odstranimo ne-commitane spremembe.
func (m *Manager) rebuildStateFromLog(entries []*privatepb.LogEntry, committed uint64) error {
	if m.state == nil {
		return nil
	}
	m.state.ResetAll()
	for _, e := range entries {
		if e == nil {
			continue
		}
		payloadMsg, err := decodePayload(e.Payload)
		if err != nil {
			return err
		}
		switch msg := payloadMsg.(type) {
		case *publicpb.User:
			m.state.UpsertUser(msg)
		case *publicpb.Topic:
			m.state.UpsertTopic(msg)
		case *publicpb.MessageEvent:
			if err := m.state.ApplyReplicatedEvent(msg); err != nil {
				return err
			}
		case *structpb.Struct:
			fields := msg.GetFields()
			kindV, ok := fields["kind"]
			if !ok {
				return fmt.Errorf("struct payload brez 'kind'")
			}
			kind := kindV.GetStringValue()
			switch kind {
			case "like_action":
				b64V, ok := fields["event_b64"]
				if !ok {
					return fmt.Errorf("like_action brez event_b64")
				}
				raw, err := base64.StdEncoding.DecodeString(b64V.GetStringValue())
				if err != nil {
					return err
				}
				var ev publicpb.MessageEvent
				if err := proto.Unmarshal(raw, &ev); err != nil {
					return err
				}
				liker := int64(fields["liker_id"].GetNumberValue())
				topicID := int64(fields["topic_id"].GetNumberValue())
				messageID := int64(fields["message_id"].GetNumberValue())
				if err := m.state.ApplyReplicatedEventWithMeta(&ev, &storage.LikeKey{TopicID: topicID, MessageID: messageID, UserID: liker}); err != nil {
					return err
				}
			default:
				return fmt.Errorf("nepodprta struct payload vrsta: %q", kind)
			}
		default:
			return fmt.Errorf("neznan payload")
		}
	}
	// Vse, kar smo replay-ali, je commitano do 'committed'.
	m.state.AdvanceCommittedSequence(int64(committed))
	return nil
}

// ResyncFromPrev izvede catch-up (Update) proti trenutnemu previous. Med resync fazo je IsReady=false.
func (m *Manager) ResyncFromPrev(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	atomic.StoreUint32(&m.syncing, 1)
	defer atomic.StoreUint32(&m.syncing, 0)

	// Ponovi, dokler ne dobimo praznega seznama (stabilno stanje), ali dokler ne poteče ctx.
	for {
		cnt, err := m.catchUpOnceCount(ctx)
		if err != nil {
			return err
		}
		if cnt == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
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
		if _, err := m.catchUpOnceCount(ctx); err == nil {
			// še enkrat do stabilnega stanja
			_ = m.ResyncFromPrev(ctx)
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

// catchUpOnceCount zahteva od predhodnika manjkajoče entry-je (Update) in jih aplicira zaporedno.
// Vrne koliko entry-jev je bilo apliciranih.
func (m *Manager) catchUpOnceCount(ctx context.Context) (int, error) {
	// Med catch-up fazo se opremo samo na COMMIT-ani del lokalnega loga.
	// Če obstajajo ne-commitani vnosi (npr. po re-konfiguraciji ali po delnem crash recovery),
	// jih ignoriramo/trunciramo, da se ne bi kasneje pomotoma "legalizirali".
	m.mu.Lock()
	if m.lastApplied > m.lastCommitted {
		if int(m.lastCommitted) < len(m.log) {
			m.log = m.log[:int(m.lastCommitted)]
		}
		m.lastApplied = m.lastCommitted
	}
	last := m.lastCommitted
	client, err := m.dialPrevLocked()
	m.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if client == nil {
		return 0, nil
	}
	resp, err := client.Update(ctx, &privatepb.UpdateRequest{LastKnownEntryId: last})
	if err != nil {
		return 0, err
	}
	missing := resp.GetMissingEntries()
	for _, e := range missing {
		if err := m.applyEntry(ctx, e, true); err != nil {
			return 0, err
		}
	}

	// Vsi entry-ji, ki jih dobimo preko Update, so po definiciji commitani (Update vrača le commitane vnose).
	// Zato lahko lokalno napredujemo committed marker do zadnjega apliciranega entry-ja.
	if len(missing) > 0 {
		m.mu.Lock()
		if m.lastApplied > m.lastCommitted {
			m.lastCommitted = m.lastApplied
		}
		committed := m.lastCommitted
		m.mu.Unlock()
		m.state.AdvanceCommittedSequence(int64(committed))
	}

	return len(missing), nil
}

func (m *Manager) isRetryableForwardErr(err error) bool {
	if err == nil {
		return false
	}
	// Naš Forward vrača plain error; zato preverimo message.
	msg := err.Error()
	if strings.Contains(msg, "out-of-order") {
		return true
	}
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "Unavailable") {
		return true
	}
	return false
}

func (m *Manager) forwardWithRetry(ctx context.Context, client privatepb.ReplicationServiceClient, entry *privatepb.LogEntry) error {
	if client == nil {
		return nil
	}
	backoff := 80 * time.Millisecond
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := client.Forward(ctx, &privatepb.ForwardRequest{Entry: entry})
		if err == nil {
			return nil
		}
		if !m.isRetryableForwardErr(err) {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 500*time.Millisecond {
				backoff *= 2
			}
		}
	}
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
	// Najprej poskusimo MessageEvent, nato User, Topic, na koncu še structpb.Struct (privatni payload).
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
	var st structpb.Struct
	if a.MessageIs(&st) {
		if err := a.UnmarshalTo(&st); err != nil {
			return nil, err
		}
		return &st, nil
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
	case *structpb.Struct:
		// Poseben "private" payload (npr. LIKE z liker_id za deterministično deduplikacijo).
		fields := msg.GetFields()
		kindV, ok := fields["kind"]
		if !ok {
			return fmt.Errorf("struct payload brez 'kind'")
		}
		kind := kindV.GetStringValue()
		switch kind {
		case "like_action":
			b64V, ok := fields["event_b64"]
			if !ok {
				return fmt.Errorf("like_action brez event_b64")
			}
			b64 := b64V.GetStringValue()
			raw, err := base64.StdEncoding.DecodeString(b64)
			if err != nil {
				return err
			}
			var ev publicpb.MessageEvent
			if err := proto.Unmarshal(raw, &ev); err != nil {
				return err
			}
			liker := int64(fields["liker_id"].GetNumberValue())
			topicID := int64(fields["topic_id"].GetNumberValue())
			messageID := int64(fields["message_id"].GetNumberValue())
			if err := m.state.ApplyReplicatedEventWithMeta(&ev, &storage.LikeKey{TopicID: topicID, MessageID: messageID, UserID: liker}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("nepodprta struct payload vrsta: %q", kind)
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
			if err := m.forwardWithRetry(ctx, client, entry); err != nil {
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
	// log.Printf("[%s] Prejeta Forward zahteva za vnos %d", m.nodeID, entry.EntryId)

	if err := m.applyEntry(ctx, entry, false); err != nil {
		// Idempotentnost + strict ordering se uveljavlja v applyEntry (EntryId mora priti zaporedno).
		return nil, err
	}

	// Posreduj naslednjemu, če obstaja.
	m.mu.Lock()
	isTail := m.isTail

	// addr za loge.
	nextAddr := m.nextAddr
	prevAddr := m.prevAddr

	nextClient, err := m.dialNextLocked()
	prevClient, prevErr := m.dialPrevLocked()
	m.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if prevErr != nil {
		return nil, prevErr
	}

	var committed uint64
	log.Printf("[%s] Posredujem vnos %d naprej na %s", m.nodeID, entry.EntryId, nextAddr)

	if nextClient != nil {
		backoff := 80 * time.Millisecond
		deadline := time.Now().Add(3 * time.Second)
		for {
			resp, err := nextClient.Forward(ctx, req)
			if err == nil {
				committed = resp.GetCommittedEntryId()
				break
			}
			if !m.isRetryableForwardErr(err) || time.Now().After(deadline) {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				if backoff < 500*time.Millisecond {
					backoff *= 2
				}
			}
		}
	}

	if isTail {
		// Na repu verige se entry smatra kot committed takoj, ko je apliciran na tail-u.
		// Nato se Commit sinhrono pošlje nazaj prejšnjemu node-u (da ohranimo ordering commitov).

		log.Printf("[%s] Rep verige: vnos %d potrjen (committed)", m.nodeID, entry.EntryId)
		committed = entry.EntryId
		// Na repu verige commitamo takoj in commit posredujemo nazaj po verigi.
		m.mu.Lock()
		if committed > m.lastCommitted {
			m.lastCommitted = committed
		}
		m.mu.Unlock()
		// Na tail-u je entry commit-an takoj.
		m.state.AdvanceCommittedSequence(int64(committed))

		if prevClient != nil {
			log.Printf("[%s] Pošiljam Commit za vnos %d nazaj na %s", m.nodeID, committed, prevAddr)
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
	var toSignal []chan error
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
	prevAddr := m.prevAddr

	m.mu.Unlock()
	// Posodobi vidnost commit-anih dogodkov (gating naročnin + deduplikacija všečkov).
	m.state.AdvanceCommittedSequence(int64(commitID))
	if err != nil {
		return nil, err
	}
	for _, ch := range toSignal {
		signalWaiter(ch, nil)
	}

	// Posreduj nazaj po verigi.
	if prevClient != nil {
		log.Printf("[%s] Pošiljam Commit za vnos %d nazaj na %s", m.nodeID, commitID, prevAddr)
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

	// V catch-up/Update vračamo **samo commitane** vnose.
	// To prepreči, da bi se ne-commitani vnosi (npr. po failed Forward na HEAD-u) razširili v verigo.
	// Zato se opiramo na lastCommitted, ne na lastApplied.
	committed := m.lastCommitted
	if last >= committed {
		// Krajšnica: sledilec trdi, da ima vse commitano.
		return &privatepb.UpdateResponse{MissingEntries: nil}, nil
	}
	// entry_id je logično 1-indeksiran, v slice-u pa je shranjen 0-indeksirano.
	// TODO: 0 indeksiraj entry_id.

	// Poskrbimo, da ne presežemo commitane dolžine loga.
	// log slice je 1:1 z EntryId (EntryId=1 je index 0).
	committedLen := min(len(m.log), int(committed))

	// Zagotovimo, da je start \in [0, committedLen].
	start := max(0, int(last))
	start = min(committedLen, start)

	out := make([]*privatepb.LogEntry, 0, committedLen-start)
	for i := start; i < committedLen; i++ {
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

// rollbackHeadUncommitted odstrani vse ne-commitane vnose iz loga.
// Uporabimo ga, če je HEAD ustvaril entry, a ga ni uspel posredovati naprej.
// S tem ohranimo invariant: lastApplied == lastCommitted (na HEAD-u).
// Klicatelj NE sme držati m.mu.
func (m *Manager) rollbackHeadUncommitted() {
	var abort []chan error

	m.mu.Lock()
	if !m.isHead {
		m.mu.Unlock()
		return
	}
	if m.lastApplied <= m.lastCommitted {
		m.mu.Unlock()
		return
	}
	// Trunciramo log do commitane meje.
	if int(m.lastCommitted) < len(m.log) {
		m.log = m.log[:int(m.lastCommitted)]
	}
	m.lastApplied = m.lastCommitted
	// Generator EntryId nadaljuje od zadnjega commit-anega id.
	m.nextEntryID = m.lastCommitted
	// Počisti morebitne waiterje (defenzivno).
	for id, ch := range m.waiters {
		if id > m.lastCommitted {
			abort = append(abort, ch)
			delete(m.waiters, id)
		}
	}
	m.mu.Unlock()

	for _, ch := range abort {
		signalWaiter(ch, ErrWriteAborted)
	}
}

// forwardToNext pošlje entry naslednjemu node-u; če veriga nima naslednika, gre za commit v enovozliščni postavitvi.

func (m *Manager) forwardToNext(ctx context.Context, e *privatepb.LogEntry) error {
	m.mu.Lock()
	nextAddr := m.nextAddr
	client, err := m.dialNextLocked()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if client == nil {
		log.Printf("[%s] Ni naslednika (single mode), lokalni commit vnosa %d", m.nodeID, e.EntryId)
		// enovozliščna postavitev (samo eno vozlišče)
		// commitaj lokalno
		_, _ = m.Commit(ctx, &privatepb.CommitRequest{CommittedEntryId: e.EntryId})
		return nil
	}
	log.Printf("[%s] Začenjam Forward vnosa %d na %s", m.nodeID, e.EntryId, nextAddr)

	_, err = client.Forward(ctx, &privatepb.ForwardRequest{Entry: e})
	if err == nil {
		return nil
	}
	if !m.isRetryableForwardErr(err) {
		return err
	}
	return m.forwardWithRetry(ctx, client, e)
}

// waitForCommit blokira (ali do ctx cancel), dokler ne prejmemo Commit za entryID.

func (m *Manager) waitForCommit(ctx context.Context, entryID uint64) error {
	m.mu.Lock()
	if m.lastCommitted >= entryID {
		m.mu.Unlock()
		return nil
	}
	ch := make(chan error, 1)
	// Kanal se zaključi v Commit() (nil) ali ob abortu re-konfiguracije (non-nil).
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
	case err := <-ch:
		return err
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

	m.mu.Lock()
	id := m.nextLogEntryIDLocked()
	// Za pravilno "commit gating" naročnin uporabljamo entry_id kot MessageEvent.sequence_number.
	ev.SequenceNumber = int64(id)
	payload, err := encodePayload(ev)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	entry := &privatepb.LogEntry{EntryId: id, Timestamp: timestamppb.Now(), Payload: payload}
	m.appendLocalEntryLocked(entry)
	m.mu.Unlock()

	if err := m.forwardToNext(ctx, entry); err != nil {
		// Forward ni uspel -> ta entry NI commit-an; na HEAD-u ga odstranimo iz loga,
		// da kasnejši Update/Resync ne bi razširil "ghost" vnosa.
		m.rollbackHeadUncommitted()
		return err
	}
	return m.waitForCommit(ctx, id)
}

// ReplicateLikeEvent replicira OP_LIKE skupaj z liker_id (za deterministično deduplikacijo po failoverju).
// Payload je structpb.Struct z base64 serializiranim MessageEvent-om in liker_id.
func (m *Manager) ReplicateLikeEvent(ctx context.Context, ev *publicpb.MessageEvent, likerID int64) error {
	if ev == nil {
		return fmt.Errorf("nil event")
	}
	if err := m.ensureHead(); err != nil {
		return err
	}

	m.mu.Lock()
	id := m.nextLogEntryIDLocked()
	// entry_id uporabimo tudi kot sequence_number (commit gating).
	ev.SequenceNumber = int64(id)

	rawEv, err := proto.Marshal(ev)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	st, err := structpb.NewStruct(map[string]any{
		"kind":       "like_action",
		"event_b64":  base64.StdEncoding.EncodeToString(rawEv),
		"liker_id":   float64(likerID),
		"topic_id":   float64(ev.GetMessage().GetTopicId()),
		"message_id": float64(ev.GetMessage().GetId()),
	})
	if err != nil {
		m.mu.Unlock()
		return err
	}
	payload, err := encodePayload(st)
	if err != nil {
		m.mu.Unlock()
		return err
	}

	entry := &privatepb.LogEntry{EntryId: id, Timestamp: timestamppb.Now(), Payload: payload}
	m.appendLocalEntryLocked(entry)
	m.mu.Unlock()

	if err := m.forwardToNext(ctx, entry); err != nil {
		m.rollbackHeadUncommitted()
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
		m.rollbackHeadUncommitted()
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
		m.rollbackHeadUncommitted()
		return err
	}
	return m.waitForCommit(ctx, id)
}
