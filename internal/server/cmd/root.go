package cmd

import (
	"fmt"
	"os"

	streznik "github.com/FedjaMocnik/razpravljalnica/internal/server"
	"github.com/spf13/cobra"
)

var naslov string

var rootCmd = &cobra.Command{
	Use:          "server",
	Short:        "Zažene gRPC strežnik Razpravljalnice",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Zaganjam Razpravljalnico na %s ...\n", naslov)
		return streznik.Zazeni(naslov)
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
}
