package carbites

import (
	"bytes"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	util "github.com/ipld/go-car/util"
)

type carMerger struct {
	cars  []*car.CarReader
	index int
	buf   *bytes.Buffer
	seen  map[cid.Cid]struct{}
}

// NewCarMerger creates a new CAR file (an io.Reader) that is a result of
// merging the passed CAR files. The resultant CAR has the combined roots of the
// passed CAR files and any duplicate blocks are removed.
func NewCarMerger(in []io.Reader) (io.Reader, error) {
	seenRoots := make(map[cid.Cid]struct{})
	var roots []cid.Cid
	var cars []*car.CarReader
	for _, r := range in {
		car, err := car.NewCarReader(r)
		if err != nil {
			return nil, err
		}
		for _, c := range car.Header.Roots {
			if _, ok := seenRoots[c]; !ok {
				roots = append(roots, c)
				seenRoots[c] = struct{}{}
			}
		}
		cars = append(cars, car)
	}

	var b []byte
	buf := bytes.NewBuffer(b)
	err := car.WriteHeader(&car.CarHeader{
		Roots:   roots,
		Version: 1,
	}, buf)
	if err != nil {
		return nil, err
	}

	return &carMerger{
		cars: cars,
		buf:  buf,
		seen: make(map[cid.Cid]struct{}),
	}, nil
}

func (mcr *carMerger) nextCar() *car.CarReader {
	mcr.index++
	if mcr.index >= len(mcr.cars) {
		return nil
	}
	return mcr.cars[mcr.index]
}

func (mcr *carMerger) nextBlock() (blocks.Block, error) {
	car := mcr.cars[mcr.index]
	for {
		blk, err := car.Next()
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			car = mcr.nextCar()
			if car == nil {
				return nil, io.EOF
			}
			continue
		}
		if _, ok := mcr.seen[blk.Cid()]; ok {
			continue
		}
		mcr.seen[blk.Cid()] = struct{}{}
		return blk, nil
	}
}

func (mcr *carMerger) Read(p []byte) (int, error) {
	if mcr.buf.Len() > 0 {
		n, err := mcr.buf.Read(p)
		if err == io.EOF {
			return n, nil
		}
		return n, err
	}

	blk, err := mcr.nextBlock()
	if err != nil {
		return 0, err
	}

	var b []byte
	buf := bytes.NewBuffer(b)
	util.LdWrite(buf, blk.Cid().Bytes(), blk.RawData())

	n, err := buf.Read(p)
	if buf.Len() > 0 {
		mcr.buf = buf
	}

	return n, err
}
