package main

import (
	"bufio"
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
		path := c.Args().First()

		out := make(chan io.Reader)
		dir := filepath.Dir(path)
		name := strings.TrimRight(filepath.Base(path), ".car")

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			var i int
			for r := range out {
				path := fmt.Sprintf("%s/%s-%d.car", dir, name, i)
				fmt.Printf("Writing CAR chunk to %s\n", path)
				fi, err := os.Create(path)
				if err != nil {
					panic(err)
				}
				br := bufio.NewReader(r)
				br.WriteTo(fi)
				fi.Close()
				i++
			}
		}()

		var strategy carbites.Strategy
		if c.String("strategy") == "treewalk" {
			strategy = carbites.Treewalk
		}
		size := c.Int("size")
		fmt.Printf("Splitting into ~%d byte chunks using strategy \"%s\"\n", size, c.String("strategy"))

		if strategy == carbites.Treewalk {
			err := carbites.SplitTreewalkFromPath(c.Context, path, size, out)
			if err != nil {
				return err
			}
		} else {
			fi, err := os.Open(path)
			if err != nil {
				return err
			}
			defer fi.Close()
			err = carbites.Split(c.Context, fi, size, strategy, out)
			if err != nil {
				return err
			}
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
			strategy = carbites.Treewalk
		}

		out, err := carbites.Join(in, strategy)
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
