package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/alanshaw/go-carbites"

	cli "github.com/urfave/cli/v2"
)

var splitCmd = &cli.Command{
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
					fi, err := os.Create(fmt.Sprintf("%s/%s-%d.car", dir, name, i))
					if err != nil {
						panic(err)
					}
					defer fi.Close()
					br := bufio.NewReader(r)
					br.WriteTo(fi)
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
			Name:    "strategy",
			Aliases: []string{"t"},
			Value:   "simple",
			Usage:   "Strategy for splitting CAR files \"simple\" or \"treewalk\".",
		},
		&cli.IntFlag{
			Name:    "size",
			Aliases: []string{"s"},
			Value:   1024 * 1024,
			Usage:   "Target size in bytes to chunk CARs to.",
		},
	},
}

var joinCmd = &cli.Command{
	Name: "join",
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			return fmt.Errorf("must pass CAR files to join")
		}
		paths := c.Args()

		var in []io.Reader

		for _, p := range paths.Slice() {
			fi, err := os.Open(p)
			if err != nil {
				return err
			}
			defer fi.Close()
			in = append(in, bufio.NewReader(fi))
		}

		var strategy carbites.Strategy
		if c.String("strategy") == "treewalk" {
			strategy = carbites.TreeWalk
		}

		out, err := carbites.Join(context.Background(), in, strategy)
		if err != nil {
			return err
		}

		fi, err := os.Create(c.String("output"))
		if err != nil {
			return err
		}
		defer fi.Close()
		br := bufio.NewReader(out)
		br.WriteTo(fi)

		return nil
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "strategy",
			Aliases: []string{"t"},
			Value:   "simple",
			Usage:   "Strategy for splitting CAR files \"simple\" or \"treewalk\".",
		},
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: true,
			Usage:    "Output path for joined CAR.",
		},
	},
}

func main() {
	app := cli.NewApp()
	app.Name = "carbites"
	app.Usage = "Chunking for CAR files. Split a single CAR into multiple CARs."
	app.Commands = []*cli.Command{
		splitCmd,
		joinCmd,
	}
	app.Run(os.Args)
}
