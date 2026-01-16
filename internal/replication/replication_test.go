package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/FedjaMocnik/razpravljalnica/internal/storage"
	privatepb "github.com/FedjaMocnik/razpravljalnica/pkgs/private/pb"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
)

// setupNode je pomožna funkcija za zagon testnega vozlišča.
// Vrne: manager, grpcServer (za zaustavitev), naslov poslušalca in funkcijo za začetek serviranja.
func setupNode(t *testing.T, nodeID string, isHead, isTail bool, prevAddr, nextAddr string) (*Manager, *grpc.Server, string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("ni mogoče poslušati: %v", err)
	}
	addr := lis.Addr().String()

	st := storage.NewState()
	cfg := Config{
		NodeID:   nodeID,
		Address:  addr,
		PrevAddr: prevAddr,
		NextAddr: nextAddr,
		IsHead:   isHead,
		IsTail:   isTail,
	}
	mgr := NewManager(cfg, st)

	s := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s, mgr)

	serveFunc := func() {
		go func() {
			if err := s.Serve(lis); err != nil {
				// To je pričakovano ob s.Stop()
			}
		}()
	}

	return mgr, s, addr, serveFunc
}

// TestSingleNodeReplication preveri delovanje replikacije na enem samem vozlišču (ki je hkrati GLAVA in REP).
// Preveri, ali se spremembe pravilno zapišejo v lokalno stanje.
func TestSingleNodeReplication(t *testing.T) {
	mgr, srv, _, start := setupNode(t, "node1", true, true, "", "")
	start()
	defer srv.Stop()
	defer mgr.Close()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	user := &publicpb.User{Name: "TestUser"}
	if err := mgr.ReplicateUser(ctx, user); err != nil {
		t.Fatalf("ReplicateUser ni uspel: %v", err)
	}

	mgr.mu.Lock()
	lastApp := mgr.lastApplied
	lastCom := mgr.lastCommitted
	mgr.mu.Unlock()

	if lastApp != 1 {
		t.Errorf("Pričakovan lastApplied 1, dobljen %d", lastApp)
	}
	if lastCom != 1 {
		t.Errorf("Pričakovan lastCommitted 1, dobljen %d", lastCom)
	}
}

// TestTwoNodeChain preveri replikacijo med dvema vozliščema (GLAVA -> REP).
// Glava mora posredovati zapis na Rep, Rep mora potrditi (Commit) nazaj na Glavo.
func TestTwoNodeChain(t *testing.T) {
	lisHead, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen head: %v", err)
	}
	addrHead := lisHead.Addr().String()

	lisTail, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen tail: %v", err)
	}
	addrTail := lisTail.Addr().String()

	stHead := storage.NewState()
	mgrHead := NewManager(Config{
		NodeID:   "head",
		Address:  addrHead,
		NextAddr: addrTail,
		IsHead:   true,
		IsTail:   false,
	}, stHead)

	stTail := storage.NewState()
	mgrTail := NewManager(Config{
		NodeID:   "tail",
		Address:  addrTail,
		PrevAddr: addrHead,
		IsHead:   false,
		IsTail:   true,
	}, stTail)

	serverHead := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(serverHead, mgrHead)
	go serverHead.Serve(lisHead)
	defer serverHead.Stop()
	defer mgrHead.Close()

	serverTail := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(serverTail, mgrTail)
	go serverTail.Serve(lisTail)
	defer serverTail.Stop()
	defer mgrTail.Close()

	mgrHead.markReady()
	mgrTail.markReady()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	topic := &publicpb.Topic{Id: 100, Name: "PorazdeljeniSistemi"}
	if err := mgrHead.ReplicateTopic(ctx, topic); err != nil {
		t.Fatalf("ReplicateTopic ni uspel: %v", err)
	}

	mgrHead.mu.Lock()
	headApp, headCom := mgrHead.lastApplied, mgrHead.lastCommitted
	mgrHead.mu.Unlock()

	if headApp != 1 || headCom != 1 {
		t.Errorf("Stanje GLAVE se ne ujema: applied=%d, committed=%d (pričakovano 1, 1)", headApp, headCom)
	}

	time.Sleep(50 * time.Millisecond)
	mgrTail.mu.Lock()
	tailApp, tailCom := mgrTail.lastApplied, mgrTail.lastCommitted
	mgrTail.mu.Unlock()

	if tailApp != 1 || tailCom != 1 {
		t.Errorf("Stanje REPA se ne ujema: applied=%d, committed=%d (pričakovano 1, 1)", tailApp, tailCom)
	}

	if tObj := stTail.GetTopic(100); tObj == nil || tObj.Name != "PorazdeljeniSistemi" {
		t.Errorf("Tema ni bila najdena v stanju repa")
	}
}

// TestBootstrap preveri, da se novo vozlišče (npr. Rep), ki se priključi kasneje,
// uspešno sinhronizira (bootstrap) iz predhodnika (Head).
func TestBootstrap(t *testing.T) {
	lisHead, _ := net.Listen("tcp", "localhost:0")
	addrHead := lisHead.Addr().String()

	lisTail, _ := net.Listen("tcp", "localhost:0")
	addrTail := lisTail.Addr().String()

	stHead := storage.NewState()
	mgrHead := NewManager(Config{
		NodeID:   "head",
		Address:  addrHead,
		NextAddr: addrTail,
		IsHead:   true,
		IsTail:   false,
	}, stHead)
	mgrHead.markReady()

	srvHead := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(srvHead, mgrHead)
	go srvHead.Serve(lisHead)
	defer srvHead.Stop()
	defer mgrHead.Close()

	ctx := context.Background()
	user := &publicpb.User{Id: 1, Name: "Alicija"}
	payload, _ := encodePayload(user)
	entry := &privatepb.LogEntry{EntryId: 1, Payload: payload}

	mgrHead.mu.Lock()
	mgrHead.appendLocalEntryLocked(entry)
	stHead.UpsertUser(user)
	mgrHead.mu.Unlock()

	stTail := storage.NewState()
	mgrTail := NewManager(Config{
		NodeID:   "tail",
		Address:  addrTail,
		PrevAddr: addrHead,
		IsHead:   false,
		IsTail:   true,
	}, stTail)

	srvTail := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(srvTail, mgrTail)
	go srvTail.Serve(lisTail)
	defer srvTail.Stop()
	defer mgrTail.Close()

	if err := mgrTail.BootstrapFromPrev(ctx); err != nil {
		t.Fatalf("Bootstrap ni uspel: %v", err)
	}

	if u := stTail.GetUser(1); u == nil || u.Name != "Alicija" {
		t.Errorf("Rep nima uporabnika Alicija po bootstrapu")
	}

	mgrTail.mu.Lock()
	if mgrTail.lastApplied != 1 {
		t.Errorf("LastApplied na repu se ne ujema: dobljen %d, pričakovan 1", mgrTail.lastApplied)
	}
	mgrTail.mu.Unlock()
}

// TestReplicationThreeNodes preveri verižno replikacijo s tremi vozlišči (Glava -> Sredina -> Rep).
func TestReplicationThreeNodes(t *testing.T) {
	l1, _ := net.Listen("tcp", "localhost:0")
	l2, _ := net.Listen("tcp", "localhost:0")
	l3, _ := net.Listen("tcp", "localhost:0")
	a1, a2, a3 := l1.Addr().String(), l2.Addr().String(), l3.Addr().String()

	mgr1 := NewManager(Config{NodeID: "1", Address: a1, NextAddr: a2, IsHead: true}, storage.NewState())
	mgr2 := NewManager(Config{NodeID: "2", Address: a2, PrevAddr: a1, NextAddr: a3}, storage.NewState())
	mgr3 := NewManager(Config{NodeID: "3", Address: a3, PrevAddr: a2, IsTail: true}, storage.NewState())

	mgr1.markReady()
	mgr2.markReady()
	mgr3.markReady()

	s1 := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s1, mgr1)
	go s1.Serve(l1)
	s2 := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s2, mgr2)
	go s2.Serve(l2)
	s3 := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s3, mgr3)
	go s3.Serve(l3)

	defer func() {
		s1.Stop()
		s2.Stop()
		s3.Stop()
		mgr1.Close()
		mgr2.Close()
		mgr3.Close()
	}()

	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	topic := &publicpb.Topic{Id: 55, Name: "VerigaProp"}
	if err := mgr1.ReplicateTopic(ctx, topic); err != nil {
		t.Fatalf("Replikacija ni uspela: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	mgr1.mu.Lock()
	lc1 := mgr1.lastCommitted
	mgr1.mu.Unlock()
	if lc1 != 1 {
		t.Errorf("Glava committed %d, pričakovano 1", lc1)
	}

	mgr2.mu.Lock()
	la2 := mgr2.lastApplied
	mgr2.mu.Unlock()
	if la2 != 1 {
		t.Errorf("Sredina applied %d, pričakovano 1", la2)
	}
}

// TestWaitForCommit preveri mehanizem čakanja na commit (waitForCommit) na strani Glave.
func TestWaitForCommit(t *testing.T) {
	l, _ := net.Listen("tcp", "localhost:0")
	mgr := NewManager(Config{NodeID: "1", Address: l.Addr().String(), IsHead: true, IsTail: true}, storage.NewState())
	mgr.markReady()

	s := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s, mgr)
	go s.Serve(l)
	defer s.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan error)
	go func() {
		err := mgr.ReplicateUser(ctx, &publicpb.User{Id: 99, Name: "Hitri"})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Napaka pri zagotavljanju commita: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("ReplicateUser je potekel (timeout)")
	}
}

// TestMiddleNodeRollbackFromState preveri, da se ob rekonfiguraciji ne-commitani vpisi
// pravilno odstranijo iz lokalnega stanja (state), ne samo iz loga.
func TestMiddleNodeRollbackFromState(t *testing.T) {
	// topo z 3mi nodi.
	l1, _ := net.Listen("tcp", "localhost:0")
	l2, _ := net.Listen("tcp", "localhost:0")
	l3, _ := net.Listen("tcp", "localhost:0")
	a1, a2, a3 := l1.Addr().String(), l2.Addr().String(), l3.Addr().String()

	st1 := storage.NewState()
	st2 := storage.NewState()
	st3 := storage.NewState()

	mgr1 := NewManager(Config{NodeID: "head", Address: a1, NextAddr: a2, IsHead: true}, st1)
	mgr2 := NewManager(Config{NodeID: "middle", Address: a2, PrevAddr: a1, NextAddr: a3}, st2)
	mgr3 := NewManager(Config{NodeID: "tail", Address: a3, PrevAddr: a2, IsTail: true}, st3)

	mgr1.markReady()
	mgr2.markReady()
	mgr3.markReady()

	s1 := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s1, mgr1)
	go s1.Serve(l1)

	s2 := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s2, mgr2)
	go s2.Serve(l2)

	s3 := grpc.NewServer()
	privatepb.RegisterReplicationServiceServer(s3, mgr3)
	go s3.Serve(l3)

	defer func() {
		s1.Stop()
		s2.Stop()
		mgr1.Close()
		mgr2.Close()
	}()

	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Log("Repliciram prvi vpis (bo committed)...")
	if err := mgr1.ReplicateTopic(ctx, &publicpb.Topic{Id: 1, Name: "CommittedTema"}); err != nil {
		t.Fatalf("Replikacija commitane teme ni uspela: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if topic := st2.GetTopic(1); topic == nil {
		t.Fatal("Commitana tema manjka v Sredini")
	}
	if topic := st3.GetTopic(1); topic == nil {
		t.Fatal("Commitana tema manjka v Repu")
	}
	t.Log("Prvi vpis je uspešno commitiran v follower vozliščih")

	t.Log("Ustavljam Rep (ne bo commitan srednji)...")
	s3.Stop()
	mgr3.Close()

	time.Sleep(100 * time.Millisecond)

	// Ustvarimo entry ročno (kot bi ga HEAD poslal)
	uncommittedUser := &publicpb.User{Id: 999, Name: "NeKommitan"}
	payload, _ := encodePayload(uncommittedUser)
	entry := &privatepb.LogEntry{EntryId: 2, Payload: payload}

	// pošljemo fwd na sredino (rep mrtev!)
	ctxShort, cancelShort := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err := mgr2.Forward(ctxShort, &privatepb.ForwardRequest{Entry: entry})
	cancelShort()

	if err == nil {
		t.Log("Opozorilo: Forward je uspel (slaba!!!)")
	} else {
		t.Logf("Forward je pričakovano spodletel: %v", err)
	}

	// prebereo če je apliciran na srednjem.
	mgr2.mu.Lock()
	midApplied := mgr2.lastApplied
	midCommitted := mgr2.lastCommitted
	mgr2.mu.Unlock()

	t.Logf("Sredina PRED rollbackom: applied=%d, committed=%d", midApplied, midCommitted)

	if midApplied <= midCommitted {
		t.Fatal("Test ni veljaven: Sredina nima ne-commitanih vpisov")
	}

	userBeforeRollback := st2.GetUser(999)
	if userBeforeRollback == nil {
		t.Fatal("Uporabnik 999 bi moral biti v state-u Sredine PRED rollbackom")
	}
	t.Logf("Uporabnik 999 JE v state-u Sredine pred rollbackom: %s", userBeforeRollback.Name)

	t.Log("Rekonfiguriram: Sredina postane nov Rep...")
	_ = mgr2.UpdateTopology(a1, "", false, true) // prev=Head, next="", isTail=true

	// preverimo rollback.
	mgr2.mu.Lock()
	midAppliedAfter := mgr2.lastApplied
	midCommittedAfter := mgr2.lastCommitted
	mgr2.mu.Unlock()

	t.Logf("Sredina PO rollbacku: applied=%d, committed=%d", midAppliedAfter, midCommittedAfter)

	if midAppliedAfter != midCommittedAfter {
		t.Errorf("Po rollbacku bi moralo veljati applied == committed, dobljeno applied=%d, committed=%d",
			midAppliedAfter, midCommittedAfter)
	}

	userAfterRollback := st2.GetUser(999)
	if userAfterRollback != nil {
		t.Errorf("NAPAKA: Uporabnik 999 je še vedno v state-u Sredine po rollbacku! "+
			"Rollback ni pravilno odstranil ne-commitanih sprememb iz state-a. Ime: %s", userAfterRollback.Name)
	} else {
		t.Log("USPEH: Uporabnik 999 je bil pravilno odstranjen iz state-a po rollbacku")
	}

	// Preveri, da commitani vpis (tema 1) ŠE obstaja
	if topic := st2.GetTopic(1); topic == nil || topic.Name != "CommittedTema" {
		t.Errorf("Commitana tema je bila napačno odstranjena pri rollbacku!")
	} else {
		t.Log("Commitana tema je pravilno ohranjena po rollbacku")
	}

	// še en commit kot dokaz da še dela HEAD -> MID
	t.Log("Testiram novo verigo...")
	_ = mgr1.UpdateTopology("", a2, true, false) // Head: next = Middle (now tail)

	time.Sleep(100 * time.Millisecond)

	if err := mgr1.ReplicateUser(ctx, &publicpb.User{Id: 2, Name: "NovUporabnik"}); err != nil {
		t.Fatalf("Replikacija po rekonfiguraciji ni uspela: %v", err)
	}

	if user := st2.GetUser(2); user == nil || user.Name != "NovUporabnik" {
		t.Errorf("Nov uporabnik manjka v Sredini (zdaj Rep) po rekonfiguraciji")
	} else {
		t.Log("Nova veriga deluje pravilno")
	}

	t.Log("Test uspešno zaključen: rollback pravilno odstrani ne-commitane spremembe iz state-a!")
}
