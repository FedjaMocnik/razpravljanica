package cmd

import "github.com/spf13/cobra"

func newGetMessagesCmd() *cobra.Command {
	var (
		temaID int64
		fromID int64
		limit  int32
	)

	c := &cobra.Command{
		Use:   "get-messages",
		Short: "Izpiše sporočila iz teme",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				r, err := o.GetMessages(ctx, temaID, fromID, limit)
				if err != nil {
					return err
				}
				if len(r.Messages) == 0 {
					cmd.Println("(ni sporočil)")
					return nil
				}
				for _, m := range r.Messages {
					cmd.Printf("- id=%d user=%d likes=%d text=%q\n", m.Id, m.UserId, m.Likes, m.Text)
				}
				return nil
			})
		},
	}

	c.Flags().Int64Var(&temaID, "tema", 0, "ID teme")
	c.Flags().Int64Var(&fromID, "od", 0, "Začetni ID sporočila (0 = od začetka)")
	c.Flags().Int32Var(&limit, "limit", 0, "Največ sporočil (0 = brez omejitve)")
	_ = c.MarkFlagRequired("tema")

	return c
}
