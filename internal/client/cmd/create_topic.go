package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newCreateTopicCmd() *cobra.Command {
	var ime string

	c := &cobra.Command{
		Use:   "create-topic <ime>",
		Short: "Ustvari temo",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if ime == "" {
				if len(args) == 1 {
					ime = args[0]
				} else {
					return fmt.Errorf("manjka ime teme (podaj argument <ime> ali --ime)")
				}
			}

			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				t, err := o.CreateTopic(ctx, ime)
				if err != nil {
					return err
				}
				cmd.Printf("Ustvarjena tema: id=%d, ime=%s\n", t.Id, t.Name)
				return nil
			})
		},
	}

	c.Flags().StringVar(&ime, "ime", "", "Ime teme")
	return c
}
