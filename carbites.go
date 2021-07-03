package carbites

import (
	"context"
	"fmt"
	"io"

	car "github.com/ipld/go-car"
)

// Strategy describes how CAR files should be split.
type Strategy int

const (
	// Simple is fast but naive, only the first CAR output has a root CID,
	// subsequent CARs have a placeholder "empty" CID.
	Simple Strategy = iota
	// TreeWalk walks the DAG to pack sub-graphs into each CAR file that is
	// output. Every CAR has the same root CID, but contains a different portion
	// of the DAG.
	TreeWalk
)

// Split a CAR file and create multiple smaller CAR files.
func Split(ctx context.Context, r *car.CarReader, targetSize int, s Strategy, out chan io.Reader) error {
	switch s {
	case Simple:
		return splitSimple(ctx, r, targetSize, out)
	case TreeWalk:
		panic("not yet implemented")
	default:
		return fmt.Errorf("unknown strategy %d", s)
	}
}
