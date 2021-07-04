package carbites

import (
	"bytes"
	"context"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	car "github.com/ipld/go-car"
	util "github.com/ipld/go-car/util"
	"github.com/willscott/carbs"
)

func init() {
	ipld.Register(cid.DagProtobuf, dag.DecodeProtobufBlock)
	ipld.Register(cid.Raw, dag.DecodeRawBlock)
}

type CarBlockReader interface {
	Get(cid.Cid) (blocks.Block, error)
}

// Split a CAR file and create multiple smaller CAR files using the "treewalk"
// strategy. Note: the entire CAR will be cached in memory. Use
// SplitTreewalkFromPath or SplitTreewalkFromBlockReader for non-memory bound
// splitting.
func SplitTreewalk(ctx context.Context, r io.Reader, targetSize int, out chan io.Reader) error {
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	h, err := car.LoadCar(bs, r)
	if err != nil {
		return err
	}
	if len(h.Roots) != 1 {
		return fmt.Errorf("unexpected number of roots: %d", len(h.Roots))
	}
	return SplitTreewalkFromBlockReader(ctx, h.Roots[0], bs, targetSize, out)
}

// Split a CAR file found on disk at the given path and create multiple smaller
// CAR files using the "treewalk" strategy.
func SplitTreewalkFromPath(ctx context.Context, path string, targetSize int, out chan io.Reader) error {
	br, err := carbs.Load(path, false)
	if err != nil {
		return err
	}
	roots, err := br.Roots()
	if err != nil {
		return err
	}
	if len(roots) != 1 {
		return fmt.Errorf("unexpected number of roots: %d", len(roots))
	}
	return SplitTreewalkFromBlockReader(ctx, roots[0], br, targetSize, out)
}

// Split a CAR file (passed as a root CID and a block reader populated with the
// blocks from the CAR) and create multiple smaller CAR files using the
// "treewalk" strategy.
func SplitTreewalkFromBlockReader(ctx context.Context, root cid.Cid, br CarBlockReader, targetSize int, out chan io.Reader) error {
	defer close(out)
	b, err := addBlock(ctx, root, br, targetSize, nil, nil, out)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- b:
	}
	return nil
}

func addBlock(ctx context.Context, c cid.Cid, br CarBlockReader, targetSize int, car *bytes.Buffer, parents []blocks.Block, out chan io.Reader) (*bytes.Buffer, error) {
	blk, err := br.Get(c)
	if err != nil {
		return nil, err
	}
	if car == nil {
		parents = []blocks.Block{}
		car, err = newCar(c, parents)
		if err != nil {
			return nil, err
		}
	}
	if car.Len() > 0 && car.Len()+len(blk.RawData()) > targetSize {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case out <- car:
		}
		car, err = newCar(parents[0].Cid(), parents)
	}
	parents = append(parents, blk)
	err = util.LdWrite(car, blk.Cid().Bytes(), blk.RawData())
	if err != nil {
		return nil, err
	}
	nd, err := ipld.Decode(blk)
	for _, link := range nd.Links() {
		car, err = addBlock(ctx, link.Cid, br, targetSize, car, parents, out)
		if err != nil {
			return nil, err
		}
	}
	return car, nil
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
