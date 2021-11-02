# carbites

[![Build](https://github.com/alanshaw/go-carbites/actions/workflows/main.yml/badge.svg)](https://github.com/alanshaw/go-carbites/actions/workflows/main.yml)
[![Standard README](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg)](https://github.com/RichardLitt/standard-readme)
[![Go Report Card](https://goreportcard.com/badge/github.com/alanshaw/go-carbites)](https://goreportcard.com/report/github.com/alanshaw/go-carbites)

Chunking for [CAR files](https://ipld.io/specs/transport/car/). Split a single CAR into multiple CARs.

## Install

```sh
go get github.com/alanshaw/go-carbites
```

## Usage

Carbites supports 2 different strategies:

1. **Simple** (default) - fast but naive, only the first CAR output has a root CID, subsequent CARs have a placeholder "empty" CID. The first CAR output has roots in the header, subsequent CARs have an empty root CID [`bafkqaaa`](https://cid.ipfs.io/#bafkqaaa) as [recommended](https://ipld.io/specs/transport/car/carv1/#number-of-roots).
2. **Treewalk** - walks the DAG to pack sub-graphs into each CAR file that is output. Every CAR file has the _same_ root CID but contains a different portion of the DAG. The DAG is traversed from the root node and each block is decoded and links extracted in order to determine which sub-graph to include in each CAR.

```go
package main

import (
	"io"
	"os"
	"github.com/alanshaw/go-carbites"
)

func main() {
	bigCar, _ := os.Open("big.car")
	targetSize := 1024 * 1024 // 1MiB chunks
	strategy := carbites.Simple // also carbites.Treewalk
	spltr, _ := carbites.Split(bigCar, targetSize, strategy)

	var i int
	for {
		car, err := spltr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		b, _ := ioutil.ReadAll(car)
		ioutil.WriteFile(fmt.Sprintf("chunk-%d.car", i), b, 0644)
		i++
	}
}
```

## API

[pkg.go.dev Reference](https://pkg.go.dev/github.com/alanshaw/go-carbites)

## Related

* [Carbites in Javascript](https://github.com/nftstorage/carbites)

## Contribute

Feel free to dive in! [Open an issue](https://github.com/alanshaw/go-carbites/issues/new) or submit PRs.

## License

Dual-licensed under [MIT + Apache 2.0](https://github.com/alanshaw/go-carbites/blob/main/LICENSE.md)
