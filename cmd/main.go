package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/alanshaw/go-carbites"

	cli "github.com/urfave/cli"
)

var splitCmd = cli.Command{
	Name: "split",
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			return fmt.Errorf("must pass a CAR file to split")
		}
		arg := c.Args().First()

		fi, err := os.Open(arg)
		if err != nil {
			return err
		}
		defer fi.Close()

		out := make(chan io.Reader)
		dir := filepath.Dir(arg)
		name := strings.TrimRight(filepath.Base(arg), ".car")

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			var i int
			for {
				select {
				case r, ok := <-out:
					if !ok {
						return
					}
					b, err := ioutil.ReadAll(r)
					if err != nil {
						panic(fmt.Errorf("reading chunk: %w", err))
					}
					err = ioutil.WriteFile(fmt.Sprintf("%s/%s-%d.car", dir, name, i), b, 0644)
					if err != nil {
						panic(fmt.Errorf("writing chunk: %w", err))
					}
					i++
				}
			}
		}()

		var strategy carbites.Strategy
		if c.String("strategy") == "treewalk" {
			strategy = carbites.TreeWalk
		}

		err = carbites.Split(context.Background(), bufio.NewReader(fi), c.Int("size"), strategy, out)
		if err != nil {
			return err
		}

		wg.Wait()
		return nil
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "strategy",
			Value: "simple",
			Usage: "Strategy for splitting CAR files \"simple\" or \"treewalk\" (default simple).",
		},
		&cli.IntFlag{
			Name:  "size",
			Value: 1000,
			Usage: "Target size in bytes to chunk CARs to (default 1KB)",
		},
	},
}

func main() {
	app := cli.NewApp()
	app.Commands = []cli.Command{
		splitCmd,
	}
	app.Run(os.Args)
}
