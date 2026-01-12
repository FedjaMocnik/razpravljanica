package controlplane

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	controlpb "github.com/FedjaMocnik/razpravljalnica/pkgs/control/pb"
	publicpb "github.com/FedjaMocnik/razpravljalnica/pkgs/public/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// Konfig za respawn ubistvu.
const (
	respawnQueueSize    = 32 // Velikost bufferja za zahteve reboota.
	respawnWorkers      = 1  // Število worker gorutin.
	maxRespawnAttempts  = 5  // Maksimalno zaporednih napak preden se vdamo.
	baseBackoff         = 2 * time.Second
	maxBackoff          = 30 * time.Second
	crashWindowDuration = 10 * time.Second // Če node preživi toliko časa, resetiramo števec napak.
	serverBinaryPath    = "./server"
	nodeLogDir          = "./logs/nodes" // Direktorij za loge neodvisnih node procesov.
)

// RespawnRequest predstavlja zahtevo za ponovni zagon node-a, ki je propadel.
type RespawnRequest struct {
	NodeID      string
	Port        string
	ControlAddr string
	TokenSecret string
}

// NodeRespawnState sledi poskusom ponovnega zagona in backoff-u za posamezen node.
type NodeRespawnState struct {
	Attempts     int
	LastAttempt  time.Time
	LastSuccess  time.Time // Kdaj je bil node nazadnje uspešno zagnan.
	BackoffUntil time.Time // Ne poskušaj ponovnega zagona do tega časa.
	GivenUp      bool      // True če smo presegli maksimalno število poskusov.
}

// SpawnedProcess sledi procesu serverja, ki smo ga zagnali.
type SpawnedProcess struct {
	PID       int
	NodeID    string
	Port      string
	StartTime time.Time
	Cmd       *exec.Cmd
}

// Respawner upravlja avtomatski ponovni zagon node-ov, ki so propadli.
type Respawner struct {
	mu sync.Mutex

	vrsta      chan RespawnRequest
	states     map[string]*NodeRespawnState // nodeID -> state
	spawned    map[string]*SpawnedProcess   // nodeID -> informacije o zagnanem procesu
	portsInUse map[string]string            // port -> nodeID

	controlAddr string
	tokenSecret string
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// Konstrukter za Respawner.
func NewRespawner(controlAddr, tokenSecret string) *Respawner {
	return &Respawner{
		vrsta:       make(chan RespawnRequest, respawnQueueSize),
		states:      make(map[string]*NodeRespawnState),
		spawned:     make(map[string]*SpawnedProcess),
		portsInUse:  make(map[string]string),
		controlAddr: controlAddr,
		tokenSecret: tokenSecret,
		stopCh:      make(chan struct{}),
	}
}

// Start zažene worker pool za ponovni zagon.
func (r *Respawner) Start() {
	// Preveri če server binary obstaja
	if _, err := os.Stat(serverBinaryPath); os.IsNotExist(err) {
		absPath, _ := filepath.Abs(serverBinaryPath)
		log.Printf("respawner WARNING: server binary ni najden na %s (CWD: %s). Ponovni zagon bo neuspešen!", serverBinaryPath, absPath)
	}

	// for i := 0; i < respawnWorkers; i++ {
	// 	r.wg.Add(1)
	// 	go r.worker(i)
	// }

	for i := range respawnWorkers {
		r.wg.Add(1)
		go r.worker(i)
	}

	log.Printf("respawner: zagnan %d worker(s), velikost queue=%d", respawnWorkers, respawnQueueSize)
}

// Stop ustavi respawner worker-je.
func (r *Respawner) Stop() {
	close(r.stopCh)
	r.wg.Wait()
	// zdej pustimo serverje na mir.

	log.Printf("respawner: ustavljen. Neodvisni node procesi še vedno tečejo.")
}

// StopAndKillNodes ustavi respawner IN ubije vse znane node.
func (r *Respawner) StopAndKillNodes() {
	close(r.stopCh)
	r.wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	// ustavimo preko PID.
	for nodeID, sp := range r.spawned {
		if sp.PID > 0 {
			log.Printf("respawner: poskušam ubiti neodvisen proces %s (PID %d)", nodeID, sp.PID)

			// signal za shutdown.
			proc, err := os.FindProcess(sp.PID)
			if err == nil && proc != nil {
				if err := proc.Signal(syscall.SIGTERM); err != nil {
					log.Printf("respawner: SIGTERM ni uspel za %s (PID %d): %v", nodeID, sp.PID, err)
					_ = proc.Kill()
				}
			}
		}
	}
}

// Dodamo nov req v vrsto preko kanala.
func (r *Respawner) RequestRespawn(nodeID, port string) {
	req := RespawnRequest{
		NodeID:      nodeID,
		Port:        port,
		ControlAddr: r.controlAddr,
		TokenSecret: r.tokenSecret,
	}

	select {
	case r.vrsta <- req:
		log.Printf("respawner: zahteva za ponovni zagon node-a %s (port %s) dodana v vrsto", nodeID, port)
	default:
		log.Printf("respawner: vrsta polna, zavržem zahtevo za ponovni zagon node-a %s", nodeID)
	}
}

// Register ubistvu samo poskrbi, da shranimo vse podatke o
// nodih, ki smo jih ročno zagnali -> to so kandidati za ponovno zaganjanje.
func (r *Respawner) RegisterNode(nodeID, port string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.states[nodeID]; !exists {
		r.states[nodeID] = &NodeRespawnState{
			LastSuccess: time.Now(),
		}
	}
	r.portsInUse[port] = nodeID
	log.Printf("respawner: registriran node %s na portu %s", nodeID, port)
}

// GetSpawnedPRocesses je getter za info o zagnanih procesih.
func (r *Respawner) GetSpawnedProcesses() map[string]SpawnedProcess {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make(map[string]SpawnedProcess, len(r.spawned))
	for k, v := range r.spawned {
		if v != nil {
			result[k] = *v
		}
	}
	return result
}

// GetNodeState vrne stanje ponovnega zagona za node.
func (r *Respawner) GetNodeState(nodeID string) *NodeRespawnState {
	r.mu.Lock()
	defer r.mu.Unlock()

	if state, ok := r.states[nodeID]; ok {
		copy := *state
		return &copy
	}
	return nil
}

// worker procesira zahteve za ponovni zagon iz vrste.
// to kličemo kot goroutino in čakamo, da konča z Wg.
func (r *Respawner) worker(id int) {
	defer r.wg.Done()
	log.Printf("respawner: worker %d zagnan", id)

	for {
		select {
		// Prejeli signal, da nehamo procese.
		case <-r.stopCh:
			log.Printf("respawner: worker %d se zaustavlja", id)
			return
		case req := <-r.vrsta:
			r.processRespawn(req)
		}
	}
}

// processRespawn obdela posamezno zahtevo za ponovni zagon z backoff in retry logiko.
func (r *Respawner) processRespawn(req RespawnRequest) {
	r.mu.Lock()

	// Pridobi ali ustvari stanje respawna za spec. node.
	state, exists := r.states[req.NodeID]
	if !exists {
		state = &NodeRespawnState{}
		r.states[req.NodeID] = state
	}

	// Preveri če smo se že vdali.
	if state.GivenUp {
		r.mu.Unlock()
		log.Printf("respawner: node %s označen kot vdan, ignoriram zahtevo za ponovni zagon", req.NodeID)
		return
	}

	// Preveri če smo še vedno v backoff intervalu.
	now := time.Now()
	if now.Before(state.BackoffUntil) {
		r.mu.Unlock()
		waitTime := state.BackoffUntil.Sub(now)
		log.Printf("respawner: node %s v backoff-u, čakam %v pred ponovnim poskusom", req.NodeID, waitTime)

		// Preveri, če smo dobili signal, da nehamo procese.
		select {
		case <-r.stopCh:
			return
		case <-time.After(waitTime):
		}

		r.mu.Lock()
	}

	// Preveri če je node preživel dovolj dolgo da resetiramo števec napak.
	if !state.LastSuccess.IsZero() && now.Sub(state.LastSuccess) > crashWindowDuration {
		if state.Attempts > 0 {
			log.Printf("respawner: node %s je preživel %v, resetiram števec napak", req.NodeID, crashWindowDuration)
			state.Attempts = 0
		}
	}

	// Preveri če smo presegli maksimalno število poskusov.
	if state.Attempts >= maxRespawnAttempts {
		state.GivenUp = true
		r.mu.Unlock()
		log.Printf("respawner: node %s je presegel maksimalno število poskusov ponovnega zagona (%d), se vdajam", req.NodeID, maxRespawnAttempts)
		return
	}

	state.Attempts++
	state.LastAttempt = now

	// Izračunaj naslednji backoff.
	backoff := baseBackoff * time.Duration(1<<uint(state.Attempts-1)) // shift -> exp
	backoff = min(backoff, maxBackoff)
	state.BackoffUntil = now.Add(backoff)

	r.mu.Unlock()

	log.Printf("respawner: poskušam ponovno zagnati node %s (poskus %d/%d, naslednji backoff: %v)",
		req.NodeID, state.Attempts, maxRespawnAttempts, backoff)

	// Dejansko zaženi proces.
	if err := r.spawnProcess(req); err != nil {
		log.Printf("respawner: neuspešno zaganjanje node-a %s: %v", req.NodeID, err)
		return
	}

	r.mu.Lock()
	state.LastSuccess = time.Now()
	r.mu.Unlock()

	log.Printf("respawner: uspešno zagnan node %s", req.NodeID)
}

// spawnProcess zažene proces serverja kot neodvisen proces.
func (r *Respawner) spawnProcess(req RespawnRequest) error {
	// Sestavi argumente za ukaz:
	// ./server --naslov localhost:PORT --node-id NODEID --control CONTROL_ADDR --token-secret SECRET

	args := []string{
		"--naslov", fmt.Sprintf("localhost:%s", req.Port),
		"--node-id", req.NodeID,
		"--control", req.ControlAddr,
	}
	if req.TokenSecret != "" {
		args = append(args, "--token-secret", req.TokenSecret)
	}

	cmd := exec.Command(serverBinaryPath, args...)

	// nastavimo da požene neodvisen proces.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Ustvari log direktorij če ne obstaja.
	if err := os.MkdirAll(nodeLogDir, 0755); err != nil {
		return fmt.Errorf("ne morem ustvariti log direktorija: %w", err)
	}

	// Sam preusmeri out na logge    .
	logPath := filepath.Join(nodeLogDir, fmt.Sprintf("%s.log", req.NodeID))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("ne morem odpreti log datoteke: %w", err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("neuspel poskus procesa: %w", err)
	}

	pid := cmd.Process.Pid

	// Po Release() proces postane "orphan" in ga prevzame init (PID 1).
	if err := cmd.Process.Release(); err != nil {
		log.Printf("respawner: opozorilo - Release() ni uspel za node %s: %v", req.NodeID, err)
	}

	logFile.Close()

	r.mu.Lock()
	r.spawned[req.NodeID] = &SpawnedProcess{
		PID:       pid,
		NodeID:    req.NodeID,
		Port:      req.Port,
		StartTime: time.Now(),
		Cmd:       nil,
	}
	r.portsInUse[req.Port] = req.NodeID
	r.mu.Unlock()

	log.Printf("respawner: zagnan node %s na portu %s (PID %d, log: %s)", req.NodeID, req.Port, pid, logPath)

	return nil
}

func (r *Respawner) monitorProcess(nodeID string, cmd *exec.Cmd) {
	// NOTE: NE UPORABLJAJ! - data node procesl naj bo neodvisen in zato tega ne nucamo.
	// Heartbeat timeout bo zaznal če node pade.
	log.Printf("respawner: monitorProcess je deprecated - node %s je neodvisen proces", nodeID)
}

// Server je dedicated control unit. Sprejema Join/Heartbeat od node-ov, zazna odpoved
// na podlagi heartbeat timeouta in posodobi verigo (chain). Po vsaki spremembi
// potisne nove sosede na VSA vozlišča preko UpdateNeighbors RPC.

// Poleg tega implementira tudi public ControlPlane (GetClusterState), da odjemalec
// vedno lahko najde trenutno glavo in rep.

type Server struct {
	controlpb.UnimplementedControlPlaneServiceServer
	publicpb.UnimplementedControlPlaneServer

	mu sync.Mutex

	chain  []*controlpb.NodeInfo
	lastHB map[string]time.Time

	// cache: node_id -> last known prev address (za odločitev ali je potreben catch-up).
	lastPrev map[string]string

	// cache: node_id -> port (primarno za respawning).
	nodePorts map[string]string

	// grpc clients -> nodes (za UpdateNeighbors).
	conns map[string]*grpc.ClientConn
	cps   map[string]controlpb.ControlPlaneServiceClient

	hbTimeout   time.Duration
	tokenSecret string

	// CU ima svoj respawner service, ki je implemntiran gor.
	respawner *Respawner

	// CU addr, ki ga poda repsawner procesom.
	selfAddr string
}

func New(hbTimeout time.Duration) *Server {
	return NewWithConfig(hbTimeout, "devsecret")
}

// Nov CU z Configom.
func NewWithConfig(hbTimeout time.Duration, tokenSecret string) *Server {
	if hbTimeout <= 0 {
		hbTimeout = 2 * time.Second
	}
	if tokenSecret == "" {
		tokenSecret = "devsecret"
	}
	return &Server{
		chain:       []*controlpb.NodeInfo{},
		lastHB:      map[string]time.Time{},
		lastPrev:    map[string]string{},
		nodePorts:   map[string]string{},
		conns:       map[string]*grpc.ClientConn{},
		cps:         map[string]controlpb.ControlPlaneServiceClient{},
		hbTimeout:   hbTimeout,
		tokenSecret: tokenSecret,
	}
}

func (s *Server) dialNode(addr string) (controlpb.ControlPlaneServiceClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("prazen naslov")
	}
	s.mu.Lock()
	if c, ok := s.cps[addr]; ok && c != nil {
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	cli := controlpb.NewControlPlaneServiceClient(cc)

	s.mu.Lock()
	if c, ok := s.cps[addr]; ok && c != nil {
		s.mu.Unlock()
		_ = cc.Close()
		return c, nil
	}
	s.conns[addr] = cc
	s.cps[addr] = cli
	s.mu.Unlock()
	return cli, nil
}

func (s *Server) nodeIndexLocked(nodeID string) int {
	for i, n := range s.chain {
		if n != nil && n.GetNodeId() == nodeID {
			return i
		}
	}
	return -1
}

func (s *Server) headLocked() *controlpb.NodeInfo {
	if len(s.chain) == 0 {
		return nil
	}
	return s.chain[0]
}

func (s *Server) tailLocked() *controlpb.NodeInfo {
	if len(s.chain) == 0 {
		return nil
	}
	return s.chain[len(s.chain)-1]
}

func (s *Server) Heartbeat(ctx context.Context, req *controlpb.HeartbeatRequest) (*controlpb.HeartbeatResponse, error) {
	id := req.GetNodeId()
	if id == "" {
		return &controlpb.HeartbeatResponse{Ok: false}, nil
	}
	s.mu.Lock()
	s.lastHB[id] = time.Now()
	s.mu.Unlock()
	return &controlpb.HeartbeatResponse{Ok: true}, nil
}

func (s *Server) JoinChain(ctx context.Context, req *controlpb.JoinChainRequest) (*controlpb.JoinChainResponse, error) {
	n := req.GetNode()
	if n == nil || n.GetNodeId() == "" || n.GetAddress() == "" {
		return nil, fmt.Errorf("JoinChain: neveljaven node")
	}

	s.mu.Lock()
	s.lastHB[n.GetNodeId()] = time.Now()

	// Shranjujemo porte.
	port := extractPort(n.GetAddress())
	if port != "" {
		s.nodePorts[n.GetNodeId()] = port
	}

	idx := s.nodeIndexLocked(n.GetNodeId())
	if idx >= 0 {
		// Re-join / update address, ohranimo pozicijo.
		s.chain[idx] = &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()}
	} else {
		// Privzeto dodamo na rep.
		s.chain = append(s.chain, &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
		idx = len(s.chain) - 1
	}

	var prev, next *controlpb.NodeInfo
	if idx > 0 {
		prev = s.chain[idx-1]
	}
	if idx+1 < len(s.chain) {
		next = s.chain[idx+1]
	}

	s.mu.Unlock()

	// registracija vskaega node-a, zakaj glej funk.
	if s.respawner != nil && port != "" {
		s.respawner.RegisterNode(n.GetNodeId(), port)
	}

	// Po join-u vedno razpošljemo novo konfiguracijo.
	go s.broadcastNeighbors()

	return &controlpb.JoinChainResponse{Previous: prev, Next: next, SyncFrom: prev}, nil
}

func (s *Server) GetChain(ctx context.Context, _ *controlpb.GetChainRequest) (*controlpb.GetChainResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Kopije
	out := make([]*controlpb.NodeInfo, 0, len(s.chain))
	for _, n := range s.chain {
		if n == nil {
			continue
		}
		out = append(out, &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
	}
	return &controlpb.GetChainResponse{Chain: out, Head: s.headLocked(), Tail: s.tailLocked()}, nil
}

// Public API za odjemalce.
func (s *Server) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*publicpb.GetClusterStateResponse, error) {
	s.mu.Lock()
	head := s.headLocked()
	tail := s.tailLocked()
	s.mu.Unlock()

	// Pretvorimo v public NodeInfo.
	var h, t *publicpb.NodeInfo
	if head != nil {
		h = &publicpb.NodeInfo{NodeId: head.GetNodeId(), Address: head.GetAddress()}
	}
	if tail != nil {
		t = &publicpb.NodeInfo{NodeId: tail.GetNodeId(), Address: tail.GetAddress()}
	}
	return &publicpb.GetClusterStateResponse{Head: h, Tail: t}, nil
}

// dodali, da remove vrne port.
func (s *Server) removeNode(nodeID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.nodeIndexLocked(nodeID)
	if idx < 0 {
		return ""
	}
	// poberemo port (za respawn)
	port := s.nodePorts[nodeID]

	// Damo ven iz chaina.
	s.chain = append(s.chain[:idx], s.chain[idx+1:]...)
	delete(s.lastHB, nodeID)
	// NOTE: ohranjamo jih, da jih lahko Respawner reciklira.

	return port
}

func (s *Server) broadcastNeighbors() {
	// snapshot
	s.mu.Lock()
	chain := make([]*controlpb.NodeInfo, 0, len(s.chain))
	for _, n := range s.chain {
		if n == nil {
			continue
		}
		chain = append(chain, &controlpb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()})
	}
	lastPrev := make(map[string]string, len(s.lastPrev))
	for k, v := range s.lastPrev {
		lastPrev[k] = v
	}
	s.mu.Unlock()

	for i, self := range chain {
		var prev, next *controlpb.NodeInfo
		if i > 0 {
			prev = chain[i-1]
		}
		if i+1 < len(chain) {
			next = chain[i+1]
		}

		// ampak cache je uporaben tudi za log.
		prevAddr := ""
		if prev != nil {
			prevAddr = prev.GetAddress()
		}
		_ = lastPrev[self.GetNodeId()]

		cli, err := s.dialNode(self.GetAddress())
		if err != nil {
			log.Printf("control: ne morem dial %s (%s): %v", self.GetNodeId(), self.GetAddress(), err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		_, err = cli.UpdateNeighbors(ctx, &controlpb.UpdateNeighborsRequest{Self: self, Previous: prev, Next: next})
		cancel()
		if err != nil {
			log.Printf("control: UpdateNeighbors failed %s (%s): %v", self.GetNodeId(), self.GetAddress(), err)
		}

		// cache update
		s.mu.Lock()
		s.lastPrev[self.GetNodeId()] = prevAddr
		s.mu.Unlock()
	}
}

func (s *Server) monitorFailures() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()

		var dead []string
		s.mu.Lock()
		for _, n := range s.chain {
			if n == nil {
				continue
			}
			t, ok := s.lastHB[n.GetNodeId()]
			if !ok {
				continue
			}
			if now.Sub(t) > s.hbTimeout {
				dead = append(dead, n.GetNodeId())
			}
		}
		s.mu.Unlock()

		if len(dead) == 0 {
			continue
		}
		for _, id := range dead {
			log.Printf("control: node %s timeout -> odstranim iz verige", id)
			// poberemo še port in ga damo nazaj.

			port := s.removeNode(id)

			// V kanal damo nov request z portom.
			if s.respawner != nil && port != "" {
				s.respawner.RequestRespawn(id, port)
			}
		}
		s.broadcastNeighbors()
	}
}

func (s *Server) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// Respawner rabi addr.
	s.selfAddr = addr

	// Zaženemo respawner.
	s.respawner = NewRespawner(addr, s.tokenSecret)
	s.respawner.Start()

	gs := grpc.NewServer()

	// health
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, healthSrv)

	controlpb.RegisterControlPlaneServiceServer(gs, s)
	publicpb.RegisterControlPlaneServer(gs, s)

	reflection.Register(gs)

	go s.monitorFailures()

	log.Printf("control: respawner omogočen (workers=%d, queue=%d, max_attempts=%d)",
		respawnWorkers, respawnQueueSize, maxRespawnAttempts)

	return gs.Serve(lis)
}

// Helper, ker sem imel težave z localhost:<port> -> port
func extractPort(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	return port
}

// Helper za ugasniti.
func (s *Server) Shutdown() {
	if s.respawner != nil {
		s.respawner.Stop()
	}
}
