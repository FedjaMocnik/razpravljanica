package cmd

import "github.com/spf13/cobra"

func newDeleteCmd() *cobra.Command {
	var (
		temaID      int64
		uporabnikID int64
		sporociloID int64
	)

	c := &cobra.Command{
		Use:   "delete",
		Short: "Izbriše sporočilo",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				if err := o.DeleteMessage(ctx, temaID, uporabnikID, sporociloID); err != nil {
					return err
				}
				cmd.Printf("Izbrisano sporočilo id=%d\n", sporociloID)
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
