package cmd

import "github.com/spf13/cobra"

func newUpdateCmd() *cobra.Command {
	var (
		temaID      int64
		uporabnikID int64
		sporociloID int64
		besedilo    string
	)

	c := &cobra.Command{
		Use:   "update",
		Short: "Posodobi obstoječe sporočilo",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				m, err := o.UpdateMessage(ctx, temaID, uporabnikID, sporociloID, besedilo)
				if err != nil {
					return err
				}
				cmd.Printf("Posodobljeno sporočilo: id=%d, besedilo=%q\n", m.Id, m.Text)
				return nil
			})
		},
	}

	c.Flags().Int64Var(&temaID, "tema", 0, "ID teme")
	c.Flags().Int64Var(&uporabnikID, "uporabnik", 0, "ID uporabnika")
	c.Flags().Int64Var(&sporociloID, "sporocilo", 0, "ID sporočila")
	c.Flags().StringVar(&besedilo, "besedilo", "", "Novo besedilo")
	_ = c.MarkFlagRequired("tema")
	_ = c.MarkFlagRequired("uporabnik")
	_ = c.MarkFlagRequired("sporocilo")
	_ = c.MarkFlagRequired("besedilo")

	return c
}
