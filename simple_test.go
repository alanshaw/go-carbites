package carbites

import (
	"fmt"
	"io"
	"os"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car"
)

const fiveMib = 1024 * 1024 * 5
const threeMib = 1024 * 1024 * 3
const testCarPath = "testdata/bafybeie2awhqr73gjvjpstwjs3y3riuhc6fpndye4vbpvyrez7bg7algoq.car"

type splitterCtor func(io.Reader, int) (Splitter, error)
type rootVerifierFunc func(*testing.T, int, cid.Cid, cid.Cid)

type carInfo struct {
	root   cid.Cid
	blocks []blocks.Block
}

func newCarInfo(t *testing.T, p string) *carInfo {
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
	inf := carInfo{root: cr.Header.Roots[0]}
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

func simpleVerifyRoot(t *testing.T, i int, chunkCid cid.Cid, carCid cid.Cid) {
	expectedRootCid := emptyHd.Roots[0]
	if i == 0 {
		expectedRootCid = carCid
	}
	if chunkCid != expectedRootCid {
		t.Fatalf("unexpected root CID: %s, wanted: %s", chunkCid, expectedRootCid)
	}
}

func TestSimpleSplit5Mib(t *testing.T) {
	testSplitter(t, func(r io.Reader, s int) (Splitter, error) {
		return NewSimpleSplitter(r, s)
	}, fiveMib, 1, simpleVerifyRoot)
}

func TestSimpleSplit3Mib(t *testing.T) {
	testSplitter(t, func(r io.Reader, s int) (Splitter, error) {
		return NewSimpleSplitter(r, s)
	}, threeMib, 2, simpleVerifyRoot)
}

func testSplitter(t *testing.T, ctor splitterCtor, targetSize int, expectedCars int, verifyRoot rootVerifierFunc) {
	fi, err := os.Open(testCarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()

	var srs []io.Reader
	spltr, err := ctor(fi, targetSize)
	if err != nil {
		t.Fatal(err)
	}

	for {
		r, err := spltr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		srs = append(srs, r)
	}

	if len(srs) != expectedCars {
		t.Fatal(fmt.Errorf("unexpected number of split CAR files: %d, wanted: %d", len(srs), expectedCars))
	}

	inf := newCarInfo(t, testCarPath)
	var unqBlks []blocks.Block
	var leafBlks []blocks.Block

	for i, r := range srs {
		car, err := car.NewCarReader(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(car.Header.Roots) != 1 {
			t.Fatalf("unexpected number of roots: %d", len(car.Header.Roots))
		}
		verifyRoot(t, i, car.Header.Roots[0], inf.root)

		carBlks := collectBlocks(t, car)
		for _, cb := range carBlks {
			found := false
			for _, ub := range unqBlks {
				if ub.Cid() == cb.Cid() {
					found = true
					break
				}
			}
			if !found {
				unqBlks = append(unqBlks, cb)
			}

			nd, err := ipld.Decode(cb)
			if err != nil {
				t.Fatal(err)
			}

			if len(nd.Links()) > 0 {
				continue
			}

			// check if this leaf node was already included in another CAR
			for _, lb := range leafBlks {
				if lb.Cid() == cb.Cid() {
					t.Fatal(fmt.Errorf("leaf node found in multiple CARs: %s", cb.Cid()))
				}
			}
			leafBlks = append(leafBlks, cb)
		}
	}

	if len(unqBlks) != len(inf.blocks) {
		t.Fatalf("incorrect block count: %d, wanted: %d", len(unqBlks), len(inf.blocks))
	}

	for _, ib := range inf.blocks {
		found := false
		for _, ub := range unqBlks {
			if ub.Cid() == ib.Cid() {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing block in split CARs: %s", ib.Cid())
		}
	}
}

func collectBlocks(t *testing.T, reader *car.CarReader) []blocks.Block {
	t.Helper()

	var blks []blocks.Block
	for {
		b, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		blks = append(blks, b)
	}

	return blks
}
