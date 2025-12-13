package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	naslov  string
	timeout time.Duration
)

var rootCmd = &cobra.Command{
	Use:          "client",
	Short:        "Odjemalec za Razpravljalnico",
	SilenceUsage: true,
	Long: `Odjemalec (CLI) za delo z Razpravljalnico.

Primeri:
  client --naslov localhost:9876 list-topics
  client --naslov localhost:9876 create-user "Ana"
  client --naslov localhost:9876 post --tema 1 --uporabnik 2 --besedilo "Živjo!"`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&naslov, "naslov", "localhost:9876", "Naslov strežnika (npr. localhost:9876)")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", 30*time.Second, "Timeout za RPC klice (npr. 5s, 1m). 0 = brez timeouta")

	rootCmd.AddCommand(newCreateUserCmd())
	rootCmd.AddCommand(newCreateTopicCmd())
	rootCmd.AddCommand(newPostCmd())
	rootCmd.AddCommand(newUpdateCmd())
	rootCmd.AddCommand(newDeleteCmd())
	rootCmd.AddCommand(newLikeCmd())
	rootCmd.AddCommand(newListTopicsCmd())
	rootCmd.AddCommand(newGetMessagesCmd())
	rootCmd.AddCommand(newSubscribeCmd())
	rootCmd.AddCommand(newClusterStateCmd())
}
