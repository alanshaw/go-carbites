package carbites

import (
	"bytes"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	car "github.com/ipld/go-car"
	util "github.com/ipld/go-car/util"
	"github.com/willscott/carbs"
)

func init() {
	ipld.Register(cid.DagProtobuf, dag.DecodeProtobufBlock)
	ipld.Register(cid.Raw, dag.DecodeRawBlock)
	ipld.Register(cid.DagCBOR, cbor.DecodeBlock)
}

type BlockReader interface {
	Get(cid.Cid) (blocks.Block, error)
}

type TreewalkSplitter struct {
	root       cid.Cid
	wcar       *bytes.Buffer   // the current "working" CAR
	pbs        []*pendingBlock // pending subtrees to add to the current CAR
	br         BlockReader
	targetSize int
}

// Split a CAR file and create multiple smaller CAR files using the "treewalk"
// strategy. Note: the entire CAR will be cached in memory. Use
// SplitTreewalkFromPath or SplitTreewalkFromBlockReader for non-memory bound
// splitting.
func NewTreewalkSplitter(r io.Reader, targetSize int) (*TreewalkSplitter, error) {
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	h, err := car.LoadCar(bs, r)
	if err != nil {
		return nil, err
	}
	if len(h.Roots) != 1 {
		return nil, fmt.Errorf("unexpected number of roots: %d", len(h.Roots))
	}
	return NewTreewalkSplitterFromBlockReader(h.Roots[0], bs, targetSize)
}

// Split a CAR file found on disk at the given path and create multiple smaller
// CAR files using the "treewalk" strategy.
func NewTreewalkSplitterFromPath(path string, targetSize int) (*TreewalkSplitter, error) {
	br, err := carbs.Load(path, false)
	if err != nil {
		return nil, err
	}
	roots, err := br.Roots()
	if err != nil {
		return nil, err
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("unexpected number of roots: %d", len(roots))
	}
	return NewTreewalkSplitterFromBlockReader(roots[0], br, targetSize)
}

// Split a CAR file (passed as a root CID and a block reader populated with the
// blocks from the CAR) and create multiple smaller CAR files using the
// "treewalk" strategy.
func NewTreewalkSplitterFromBlockReader(root cid.Cid, br BlockReader, targetSize int) (*TreewalkSplitter, error) {
	b, err := br.Get(root)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("missing block for CID: %s", root)
	}

	parents := []blocks.Block{b}
	wcar, err := newCar(root, parents)
	if err != nil {
		return nil, err
	}

	nd, err := ipld.Decode(b)
	if err != nil {
		return nil, err
	}

	pbs := []*pendingBlock{}
	for _, link := range nd.Links() {
		pbs = append(pbs, &pendingBlock{parents, link.Cid})
	}

	return &TreewalkSplitter{root, wcar, pbs, br, targetSize}, nil
}

func (spltr *TreewalkSplitter) Next() (io.Reader, error) {
	for {
		if len(spltr.pbs) == 0 {
			if spltr.wcar != nil {
				car := spltr.wcar
				spltr.wcar = nil
				return car, nil
			}
			break // done
		}
		st := spltr.pbs[0]
		spltr.pbs = spltr.pbs[1:]

		b, err := spltr.br.Get(st.cid)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, fmt.Errorf("missing block for CID: %s", st.cid)
		}

		readyCar, links, err := spltr.addBlock(b, spltr.wcar)
		if err != nil {
			return nil, err
		}

		parents := append(st.parents, b)

		if len(links) > 0 {
			pbs := []*pendingBlock{}
			for _, link := range links {
				pbs = append(pbs, &pendingBlock{parents, link.Cid})
			}
			spltr.pbs = append(pbs, spltr.pbs...)
		}

		if readyCar != nil {
			spltr.wcar, err = newCar(spltr.root, parents)
			if err != nil {
				return nil, err
			}
			return readyCar, nil
		}
	}

	return nil, io.EOF
}

type pendingBlock struct {
	parents []blocks.Block
	cid     cid.Cid
}

func (spltr *TreewalkSplitter) addBlock(b blocks.Block, car *bytes.Buffer) (*bytes.Buffer, []*ipld.Link, error) {
	var readyCar *bytes.Buffer
	if car.Len() > 0 && car.Len()+len(b.RawData()) > spltr.targetSize {
		readyCar = car
	}
	err := util.LdWrite(car, b.Cid().Bytes(), b.RawData())
	if err != nil {
		return nil, nil, err
	}
	nd, err := ipld.Decode(b)
	if err != nil {
		return nil, nil, err
	}
	return readyCar, nd.Links(), nil
}

func newCar(root cid.Cid, parents []blocks.Block) (*bytes.Buffer, error) {
	var b []byte
	buf := bytes.NewBuffer(b)
	err := car.WriteHeader(&car.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}, buf)
	if err != nil {
		return nil, err
	}
	for _, blk := range parents {
		err = util.LdWrite(buf, blk.Cid().Bytes(), blk.RawData())
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// Join together multiple CAR files into a single CAR file using the "treewalk"
// strategy. Note that binary equality between the original CAR and the joined
// CAR is not guaranteed.
func JoinTreewalk(in []io.Reader) (io.Reader, error) {
	return NewCarMerger(in)
}
