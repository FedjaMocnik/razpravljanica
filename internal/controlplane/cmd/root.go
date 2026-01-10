package cmd

import (
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

	// Parse peers
	var peerConfigs []cp.PeerConfig
	if peers != "" {
		if err := json.Unmarshal([]byte(peers), &peerConfigs); err != nil {
			// Poskusi enostavnejši format: node1:raft1:grpc1,node2:raft2:grpc2
			for _, p := range strings.Split(peers, ",") {
				parts := strings.Split(p, ":")
				if len(parts) >= 3 {
					peerConfigs = append(peerConfigs, cp.PeerConfig{
						NodeID:   parts[0],
						RaftAddr: parts[1] + ":" + parts[2],
						GRPCAddr: parts[3] + ":" + parts[4],
					})
				}
			}
		}
	}

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

	return srv.Serve()
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
