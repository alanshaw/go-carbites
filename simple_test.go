package carbites

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
)

const fiveMib = 1024 * 1024 * 5
const testCarPath = "testdata/bafybeie2awhqr73gjvjpstwjs3y3riuhc6fpndye4vbpvyrez7bg7algoq.car"

type CarInfo struct {
	root   cid.Cid
	blocks []blocks.Block
}

func newCarInfo(t *testing.T, p string) *CarInfo {
	t.Helper()
	fi, err := os.Open("testdata/bafybeie2awhqr73gjvjpstwjs3y3riuhc6fpndye4vbpvyrez7bg7algoq.car")
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()
	cr, err := car.NewCarReader(fi)
	if err != nil {
		t.Fatal(err)
	}
	inf := CarInfo{root: cr.Header.Roots[0]}
	for {
		blk, err := cr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		inf.blocks = append(inf.blocks, blk)
	}
	return &inf
}

func TestSimpleSplit(t *testing.T) {
	fi, err := os.Open(testCarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()

	out := make(chan io.Reader)
	var wg sync.WaitGroup
	wg.Add(1)

	var srs []io.Reader

	go func() {
		defer wg.Done()
		for r := range out {
			srs = append(srs, r)
		}
	}()

	err = SplitSimple(context.Background(), fi, fiveMib, out)
	if err != nil {
		t.Fatal(err)
	}

	inf := newCarInfo(t, testCarPath)

	for i, r := range srs {
		car, err := car.NewCarReader(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(car.Header.Roots) != 1 {
			t.Fatalf("unexpected number of roots: %d", len(car.Header.Roots))
		}
		expectedRootCid := emptyHd.Roots[0]
		if i == 0 {
			expectedRootCid = inf.root
		}
		if car.Header.Roots[0] != expectedRootCid {
			t.Fatalf("unexpected root CID: %s, wanted: %s", car.Header.Roots[0], expectedRootCid)
		}
	}
}
