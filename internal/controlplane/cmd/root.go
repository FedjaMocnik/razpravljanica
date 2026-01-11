package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	cp "github.com/FedjaMocnik/razpravljalnica/internal/controlplane"
	"github.com/spf13/cobra"
)

var (
	addr      string
	hbTimeout time.Duration
	// Raft flags
	nodeID    string
	raftAddr  string
	dataDir   string
	bootstrap bool
	peers     string
	useRaft   bool
)

var rootCmd = &cobra.Command{
	Use:          "control",
	Short:        "Zažene control unit (nadzorno ravnino) za Razpravljalnico",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if useRaft {
			return runRaftServer()
		}
		return runLegacyServer()
	},
}

func runLegacyServer() error {
	fmt.Printf("Zaganjam control unit na %s (heartbeat timeout=%s) ...\n", addr, hbTimeout)
	srv := cp.New(hbTimeout)
	return srv.Serve(addr)
}

func runRaftServer() error {
	fmt.Printf("Zaganjam Raft control unit %s (gRPC: %s, Raft: %s, bootstrap: %v)\n",
		nodeID, addr, raftAddr, bootstrap)

	peerConfigs := parsePeersFlag(peers)

	cfg := cp.RaftConfig{
		NodeID:    nodeID,
		RaftAddr:  raftAddr,
		GRPCAddr:  addr,
		DataDir:   dataDir,
		Bootstrap: bootstrap,
		Peers:     peerConfigs,
		HBTimeout: hbTimeout,
	}

	srv, err := cp.NewRaftServer(cfg)
	if err != nil {
		return fmt.Errorf("napaka pri ustvarjanju raft serverja: %w", err)
	}

	// Če to ni bootstrap node in še nima stanja na disku, poskusi auto-join na obstoječi cluster.
	// AutoJoin teče v ozadju; Serve() blokira.
	if !bootstrap {
		go srv.AutoJoin(context.Background())
	}

	return srv.Serve()
}

func parsePeersFlag(s string) []cp.PeerConfig {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var out []cp.PeerConfig
	// 1) JSON format: [{"node_id":...,"raft_addr":...,"grpc_addr":...}, ...]
	if err := json.Unmarshal([]byte(s), &out); err == nil {
		return out
	}
	// 2) Compact formats (comma-separated):
	//    - id=raftAddr=grpcAddr
	//    - id@raftAddr@grpcAddr
	//    - legacy: id:raftHost:raftPort:grpcHost:grpcPort
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if strings.Contains(tok, "=") {
			parts := strings.Split(tok, "=")
			if len(parts) >= 3 {
				out = append(out, cp.PeerConfig{NodeID: parts[0], RaftAddr: parts[1], GRPCAddr: parts[2]})
				continue
			}
		}
		if strings.Contains(tok, "@") {
			parts := strings.Split(tok, "@")
			if len(parts) >= 3 {
				out = append(out, cp.PeerConfig{NodeID: parts[0], RaftAddr: parts[1], GRPCAddr: parts[2]})
				continue
			}
		}
		parts := strings.Split(tok, ":")
		if len(parts) >= 5 {
			out = append(out, cp.PeerConfig{NodeID: parts[0], RaftAddr: parts[1] + ":" + parts[2], GRPCAddr: parts[3] + ":" + parts[4]})
			continue
		}
	}
	return out
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Legacy flags
	rootCmd.PersistentFlags().StringVar(&addr, "naslov", "localhost:9999", "Naslov gRPC strežnika (npr. localhost:9999)")
	rootCmd.PersistentFlags().DurationVar(&hbTimeout, "hb-timeout", 30*time.Second, "Heartbeat timeout za data serverje (npr. 30s)")

	// Raft flags
	rootCmd.PersistentFlags().BoolVar(&useRaft, "raft", false, "Uporabi Raft za replikacijo control plane")
	rootCmd.PersistentFlags().StringVar(&nodeID, "node-id", "control1", "ID tega control unit vozlišča")
	rootCmd.PersistentFlags().StringVar(&raftAddr, "raft-addr", "localhost:10000", "Naslov za Raft komunikacijo")
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "./data/control1", "Direktorij za Raft podatke")
	rootCmd.PersistentFlags().BoolVar(&bootstrap, "bootstrap", false, "Bootstrap nov Raft cluster")
	rootCmd.PersistentFlags().StringVar(&peers, "peers", "", "Peers v formatu JSON ali node:rafthost:raftport:grpchost:grpcport,...")
}
