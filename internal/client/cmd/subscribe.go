package cmd

import (
	"context"
	"io"

	odjemalec "github.com/FedjaMocnik/razpravljalnica/internal/client"
	"github.com/spf13/cobra"
)

func newSubscribeCmd() *cobra.Command {
	var (
		uporabnikID int64
		temeCSV     string
		fromID      int64
	)

	c := &cobra.Command{
		Use:   "subscribe",
		Short: "Naroči se na teme in sproti izpisuje dogodke",
		RunE: func(cmd *cobra.Command, args []string) error {
			teme, err := parseCSVInt64(temeCSV)
			if err != nil {
				return err
			}

			// Stream naj teče “neskončno”: ctx brez timeouta.
			ctx := context.Background()

			o, err := odjemalec.Povezi(naslov)
			if err != nil {
				return err
			}
			defer o.Zapri()

			stream, err := o.Subscribe(ctx, uporabnikID, teme, fromID)
			if err != nil {
				return err
			}

			cmd.Printf("Naročen na teme %v (od id=%d). CTRL+C za izhod.\n", teme, fromID)
			for {
				e, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
				m := e.GetMessage()
				cmd.Printf("[seq=%d op=%s tema=%d] msg_id=%d user=%d likes=%d text=%q\n",
					e.GetSequenceNumber(),
					e.GetOp().String(),
					m.GetTopicId(),
					m.GetId(),
					m.GetUserId(),
					m.GetLikes(),
					m.GetText(),
				)
			}
		},
	}

	c.Flags().Int64Var(&uporabnikID, "uporabnik", 0, "ID uporabnika")
	c.Flags().StringVar(&temeCSV, "teme", "", "Seznam tem, ločenih z vejico (npr. 1,2,3)")
	c.Flags().Int64Var(&fromID, "od", 0, "Začetni ID sporočila (0 = od začetka)")
	_ = c.MarkFlagRequired("uporabnik")
	_ = c.MarkFlagRequired("teme")

	return c
}
