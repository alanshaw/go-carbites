# carbites

[![Build](https://github.com/alanshaw/go-carbites/actions/workflows/main.yml/badge.svg)](https://github.com/alanshaw/go-carbites/actions/workflows/main.yml)
[![Standard README](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg)](https://github.com/RichardLitt/standard-readme)
[![Go Report Card](https://goreportcard.com/badge/github.com/alanshaw/multiwriter)](https://goreportcard.com/report/github.com/alanshaw/multiwriter)

Chunking for [CAR files](https://github.com/ipld/specs/blob/master/block-layer/content-addressable-archives.md). Split a single CAR into multiple CARs.

## Install

```sh
go get github.com/alanshaw/go-carbites
```

## Usage

Carbites supports 2 different strategies:

1. [**Simple**](#simple) (default) - fast but naive, only the first CAR output has a root CID, subsequent CARs have a placeholder "empty" CID.
2. [**Treewalk**](#treewalk) - walks the DAG to pack sub-graphs into each CAR file that is output. Every CAR has the same root CID, but contains a different portion of the DAG.

### Simple

The first CAR output has roots in the header, subsequent CARs have an empty root CID [`bafkqaaa`](https://cid.ipfs.io/#bafkqaaa) as [recommended](https://ipld.io/specs/transport/car/carv1/#number-of-roots).

### Treewalk

Every CAR file has the _same_ root CID but a different portion of the DAG. The DAG is traversed from the root node and each block is decoded and links extracted in order to determine which sub-graph to include in each CAR.

### Example

```go
package main

import (
	"github.com/alanshaw/go-carbites"
	"github.com/ipld/go-car"
)

func main() {
    out := make(chan io.Reader)

    go func() {
        var i int
        for {
            select {
            case r := <-out:
                b, _ := ioutil.ReadAll(r)
                ioutil.WriteFile(fmt.Sprintf("chunk-%d.car", i), b, 0644)
                i++
            }
        }
    }()

    car, _ := car.NewCarReader(reader)
    targetSize := 1000 // 1kb chunks
    strategy := carbites.Simple // also carbites.TreeWalk
    err := carbites.Split(context.Background(), car, targetSize, strategy, out)
}

```

## API

[pkg.go.dev Reference](https://pkg.go.dev/github.com/alanshaw/go-carbites)

## Contribute

Feel free to dive in! [Open an issue](https://github.com/alanshaw/go-carbites/issues/new) or submit PRs.

## License

Dual-licensed under [MIT](https://github.com/alanshaw/go-carbites/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/alanshaw/go-carbites/blob/main/LICENSE-APACHE)
