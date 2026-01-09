package cmd

import (
	"fmt"
	"os"
	"time"

	cp "github.com/FedjaMocnik/razpravljalnica/internal/controlplane"
	"github.com/spf13/cobra"
)

var (
	addr      string
	hbTimeout time.Duration
)

var rootCmd = &cobra.Command{
	Use:          "control",
	Short:        "Zažene control unit (nadzorno ravnino) za Razpravljalnico",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Zaganjam control unit na %s (heartbeat timeout=%s) ...\n", addr, hbTimeout)
		srv := cp.New(hbTimeout)
		return srv.Serve(addr)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&addr, "naslov", "localhost:9999", "Naslov control unit strežnika (npr. localhost:9999)")
	rootCmd.PersistentFlags().DurationVar(&hbTimeout, "hb-timeout", 2*time.Second, "Heartbeat timeout (npr. 2s)")
}
