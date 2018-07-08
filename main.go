package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/Mitu217/tamate/command"
	"github.com/Mitu217/tamate/differ"
	"github.com/urfave/cli"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func main() {
	// logger
	atom := zap.NewAtomicLevel() // default: Info
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.LevelKey = "" // don't show log-level
	encoderCfg.TimeKey = ""  // don't show timestamp
	logger = zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	))
	defer logger.Sync()

	// input params
	verbose := false
	datasources := &command.DatasourceConfig{
		Configs: make([]map[string]interface{}, 0),
	}

	// start app
	app := cli.NewApp()
	app.Name = "tamate"
	app.Usage = "read and write diffs between table-based data"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "verbose, v",
			Usage: "show verbose logging",
		},
		cli.StringFlag{
			Name:  "datasources, ds",
			Value: "datasources.json",
			Usage: "definitions of datasource connection information",
		},
	}
	app.Before = func(c *cli.Context) error {
		verbose = c.Bool("verbose")
		if verbose {
			atom.SetLevel(zap.DebugLevel) // show verbose log
		}

		ds := c.String("datasources")
		f, err := os.Open(ds)
		if err != nil {
			return err
		}
		logger.Debug("open", zap.String("path", ds))

		if err := json.NewDecoder(f).Decode(datasources); err != nil {
			return err
		}
		logger.Debug("decode", zap.Any("datasources", datasources))
		return nil
	}
	app.Action = func(c *cli.Context) error {
		if c.NArg() < 2 {
			return errors.New("must specify 2 datasources")
		}

		l := c.Args().Get(0)
		r := c.Args().Get(1)
		logger.Debug("target", zap.String("left", l))
		logger.Debug("target", zap.String("right", r))

		ctx := context.Background()
		columns, rows, err := datasources.GetDiff(ctx, l, r)
		if err != nil {
			fmt.Println(err)
			return err
		}
		logger.Debug("diff", zap.Any("columns", columns))
		logger.Debug("diff", zap.Any("rows", rows))

		return nil
	}
	app.HideVersion = true // disable version flag
	app.Run(os.Args)
}

func print(columns differ.DiffColumns, rows differ.DiffRows) error {
	return nil
}
