package carbites

import (
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	util "github.com/ipld/go-car/util"
)

var emptyHd *car.CarHeader

func init() {
	emptyCid, _ := cid.Decode("bafkqaaa")
	emptyHd = &car.CarHeader{
		Roots:   []cid.Cid{emptyCid},
		Version: 1,
	}
}

// Split a CAR file and create multiple smaller CAR files using the "simple"
// strategy.
func SplitSimple(ctx context.Context, in io.Reader, targetSize int, out chan io.Reader) error {
	defer close(out)
	r, err := car.NewCarReader(in)
	if err != nil {
		return err
	}
	h := r.Header
	done := false
	for {
		r, err := readChunk(ctx, h, r, targetSize)
		if err != nil {
			if r != nil && err == io.EOF {
				done = true
			} else {
				return err
			}
		}
		h = emptyHd
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- r:
		}
		if done {
			break
		}
	}
	return nil
}

func readChunk(ctx context.Context, h *car.CarHeader, carReader *car.CarReader, s int) (io.Reader, error) {
	var b []byte
	buf := bytes.NewBuffer(b)
	err := car.WriteHeader(h, buf)
	if err != nil {
		return nil, err
	}
	total := buf.Len()
	for {
		bl, err := carReader.Next()
		if err != nil {
			return buf, err
		}
		util.LdWrite(buf, bl.Cid().Bytes(), bl.RawData())
		total += len(bl.RawData())
		if total >= s {
			break
		}
	}
	return buf, nil
}

// Join together multiple CAR files that were split using the "simple" strategy
// into a single CAR file.
func JoinSimple(in []io.Reader) (io.Reader, error) {
	var brs []io.Reader
	for i, r := range in {
		br := bufio.NewReader(r)
		brs = append(brs, br)
		if i == 0 {
			continue
		}
		// discard header from other CARs
		_, err := util.LdRead(br)
		if err != nil {
			return nil, err
		}
	}
	return io.MultiReader(brs...), nil
}
