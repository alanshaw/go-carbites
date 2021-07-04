package carbites

import (
	"context"
	"fmt"
	"io"
)

// Strategy describes how CAR files should be split.
type Strategy int

const (
	// Simple is fast but naive, only the first CAR output has a root CID,
	// subsequent CARs have a placeholder "empty" CID.
	Simple Strategy = iota
	// Treewalk walks the DAG to pack sub-graphs into each CAR file that is
	// output. Every CAR has the same root CID, but contains a different portion
	// of the DAG.
	Treewalk
)

// Split a CAR file and create multiple smaller CAR files.
func Split(ctx context.Context, in io.Reader, targetSize int, s Strategy, out chan io.Reader) error {
	switch s {
	case Simple:
		return SplitSimple(ctx, in, targetSize, out)
	case Treewalk:
		return SplitTreewalk(ctx, in, targetSize, out)
	default:
		return fmt.Errorf("unknown strategy %d", s)
	}
}

// Join together multiple CAR files into a single CAR file.
func Join(in []io.Reader, s Strategy) (io.Reader, error) {
	switch s {
	case Simple:
		return JoinSimple(in)
	case Treewalk:
		return JoinTreewalk(in)
	default:
		return nil, fmt.Errorf("unknown strategy %d", s)
	}
}
