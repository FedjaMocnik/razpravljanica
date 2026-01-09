package cmd

import (
	"fmt"
	"os"

	streznik "github.com/FedjaMocnik/razpravljalnica/internal/server"
	"github.com/spf13/cobra"
)

var naslov string
var nodeID string
var chainSpec string
var tokenSecret string
var controlAddr string

var rootCmd = &cobra.Command{
	Use:          "server",
	Short:        "Zažene gRPC strežnik Razpravljalnice",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Zaganjam Razpravljalnico node=%s na %s ...\n", nodeID, naslov)
		return streznik.Zazeni(naslov, nodeID, chainSpec, tokenSecret, controlAddr)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&naslov, "naslov", "localhost:9876", "Naslov strežnika (npr. localhost:9876)")
	rootCmd.PersistentFlags().StringVar(&nodeID, "node-id", "node1", "ID vozlišča (mora biti v chain)")
	rootCmd.PersistentFlags().StringVar(&chainSpec, "chain", "node1=localhost:9876", "Veriga (head->tail): node1=host:port,node2=host:port,...")
	rootCmd.PersistentFlags().StringVar(&tokenSecret, "token-secret", "devsecret", "Skrivnost za subscribe tokene (mora biti enaka na vseh vozliščih)")
	rootCmd.PersistentFlags().StringVar(&controlAddr, "control", "", "Naslov control unit strežnika (npr. localhost:9999). Če je podan, se chain upravlja dinamično.")
}
