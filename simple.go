package carbites

import (
	"bufio"
	"bytes"
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

type SimpleSplitter struct {
	reader     *car.CarReader
	header     *car.CarHeader
	targetSize int
}

// Create a new CAR file splitter to create multiple smaller CAR files using the
// "simple" strategy.
func NewSimpleSplitter(in io.Reader, targetSize int) (*SimpleSplitter, error) {
	r, err := car.NewCarReader(in)
	if err != nil {
		return nil, err
	}
	h := r.Header
	return &SimpleSplitter{r, h, targetSize}, nil
}

func (spltr *SimpleSplitter) Next() (io.Reader, error) {
	var b []byte
	buf := bytes.NewBuffer(b)
	err := car.WriteHeader(spltr.header, buf)
	if err != nil {
		return nil, err
	}
	spltr.header = emptyHd
	empty := true
	total := buf.Len()
	for {
		bl, err := spltr.reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		empty = false
		util.LdWrite(buf, bl.Cid().Bytes(), bl.RawData())
		total += len(bl.RawData())
		if total >= spltr.targetSize {
			break
		}
	}
	if empty {
		return nil, io.EOF
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
