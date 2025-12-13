package cmd

import "github.com/spf13/cobra"

func newPostCmd() *cobra.Command {
	var (
		temaID      int64
		uporabnikID int64
		besedilo    string
	)

	c := &cobra.Command{
		Use:   "post",
		Short: "Objavi novo sporočilo v temi",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				m, err := o.PostMessage(ctx, temaID, uporabnikID, besedilo)
				if err != nil {
					return err
				}
				cmd.Printf("Objavljeno sporočilo: id=%d, tema=%d, uporabnik=%d, likes=%d\n", m.Id, m.TopicId, m.UserId, m.Likes)
				return nil
			})
		},
	}

	c.Flags().Int64Var(&temaID, "tema", 0, "ID teme")
	c.Flags().Int64Var(&uporabnikID, "uporabnik", 0, "ID uporabnika")
	c.Flags().StringVar(&besedilo, "besedilo", "", "Besedilo sporočila")
	_ = c.MarkFlagRequired("tema")
	_ = c.MarkFlagRequired("uporabnik")
	_ = c.MarkFlagRequired("besedilo")

	return c
}
