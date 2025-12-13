package cmd

import "github.com/spf13/cobra"

func newListTopicsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "list-topics",
		Short: "Izpiše vse teme",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				r, err := o.ListTopics(ctx)
				if err != nil {
					return err
				}
				if len(r.Topics) == 0 {
					cmd.Println("(ni tem)")
					return nil
				}
				for _, t := range r.Topics {
					cmd.Printf("- %d: %s\n", t.Id, t.Name)
				}
				return nil
			})
		},
	}

	return c
}
