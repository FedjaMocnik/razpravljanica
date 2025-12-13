package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newCreateUserCmd() *cobra.Command {
	var ime string

	c := &cobra.Command{
		Use:   "create-user <ime>",
		Short: "Ustvari uporabnika",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if ime == "" {
				if len(args) == 1 {
					ime = args[0]
				} else {
					return fmt.Errorf("manjka ime (podaj argument <ime> ali --ime)")
				}
			}

			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				u, err := o.CreateUser(ctx, ime)
				if err != nil {
					return err
				}
				cmd.Printf("Ustvarjen uporabnik: id=%d, ime=%s\n", u.Id, u.Name)
				return nil
			})
		},
	}

	c.Flags().StringVar(&ime, "ime", "", "Ime uporabnika")
	return c
}
