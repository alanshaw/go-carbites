package carbites

import (
	"io"
	"testing"

	"github.com/ipfs/go-cid"
)

func treewalkVerifyRoot(t *testing.T, i int, chunkCid cid.Cid, carCid cid.Cid) {
	if chunkCid != carCid {
		t.Fatalf("unexpected root CID: %s, wanted: %s", chunkCid, carCid)
	}
}

func TestTreewalkSplit5Mib(t *testing.T) {
	testSplitter(t, func(r io.Reader, s int) (Splitter, error) {
		return NewTreewalkSplitter(r, s)
	}, fiveMib, 1, treewalkVerifyRoot)
}

func TestTreewalkSplit3Mib(t *testing.T) {
	testSplitter(t, func(r io.Reader, s int) (Splitter, error) {
		return NewTreewalkSplitter(r, s)
	}, threeMib, 2, treewalkVerifyRoot)
}
