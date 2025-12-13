package cmd

import "github.com/spf13/cobra"

func newClusterStateCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "cluster-state",
		Short: "Izpiše stanje clustra (HEAD/TAIL)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := ctxWithTimeout()
			defer cancel()

			return withClient(func(o *Client) error {
				st, err := o.GetClusterState(ctx)
				if err != nil {
					return err
				}
				cmd.Printf("HEAD: %s (%s)\n", st.Head.NodeId, st.Head.Address)
				cmd.Printf("TAIL: %s (%s)\n", st.Tail.NodeId, st.Tail.Address)
				return nil
			})
		},
	}

	return c
}
