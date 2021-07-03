package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/alanshaw/go-carbites"
	"github.com/ipld/go-car"

	cli "github.com/urfave/cli"
)

var splitCmd = cli.Command{
	Name: "header",
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

		cr, err := car.NewCarReader(bufio.NewReader(fi))
		if err != nil {
			return err
		}

		out := make(chan io.Reader)
		dir := filepath.Dir(arg)
		name := strings.TrimRight(filepath.Base(arg), ".car")

		go func() {
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

		return carbites.Split(c, cr, c.Int("size"), c.Int("strategy"), out)
	},
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "strategy",
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
