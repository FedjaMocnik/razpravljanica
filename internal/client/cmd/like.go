package cmd

import "github.com/spf13/cobra"

func newLikeCmd() *cobra.Command {
	var (
		temaID      int64
		uporabnikID int64
		sporociloID int64
	)

	c := &cobra.Command{
		Use:   "like",
		Short: "Doda like na sporočilo",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				m, err := o.LikeMessage(ctx, temaID, uporabnikID, sporociloID)
				if err != nil {
					return err
				}
				cmd.Printf("Like OK: id=%d, likes=%d\n", m.Id, m.Likes)
				return nil
			})
		},
	}

	c.Flags().Int64Var(&temaID, "tema", 0, "ID teme")
	c.Flags().Int64Var(&uporabnikID, "uporabnik", 0, "ID uporabnika")
	c.Flags().Int64Var(&sporociloID, "sporocilo", 0, "ID sporočila")
	_ = c.MarkFlagRequired("tema")
	_ = c.MarkFlagRequired("uporabnik")
	_ = c.MarkFlagRequired("sporocilo")

	return c
}
