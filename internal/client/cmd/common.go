package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	odjemalec "github.com/FedjaMocnik/razpravljalnica/internal/client"
)

// Client je samo alias, da ga lahko uporabljamo v drugih cmd/*.go datotekah
// brez ponavljanja importov.
type Client = odjemalec.Odjemalec

func ctxWithTimeout() (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

func withClient(fn func(*Client) error) error {
	c, err := odjemalec.Povezi(naslov)
	if err != nil {
		return fmt.Errorf("napaka povezave: %w", err)
	}
	defer c.Zapri()
	return fn(c)
}

func parseCSVInt64(csv string) ([]int64, error) {
	parts := strings.Split(csv, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("neveljaven seznam tem (%q): %w", csv, err)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("prazen seznam tem: %q", csv)
	}
	return out, nil
}
