package main

import (
	"fmt"
	"os"

	"github.com/codegangsta/cli"
	"github.com/Mitu217/tamate/spreadSheets"
)

func main() {
	app := cli.NewApp()
	app.Name = "tamate"
	app.Usage = "tamate commands"
	app.Version = "0.1.0"

	// Global Options.
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "dryrun, d",
			Usage: "sample global option.",
		},
	}

	// Commands.
	app.Commands = []cli.Command{
		{
			Name:    "export-sheets-csv",
			Aliases: []string{"os"},
			Usage:   "",
			Action:  exportSpreadSheetAction,
		},
	}

	// Action before commands exec.
	app.Before = func(c *cli.Context) error {
		return nil
	}

	// Action after commands exec.
	app.After = func(c *cli.Context) error {
		return nil
	}

	// Run Commands.
	app.Run(os.Args)
}

func exportSpreadSheetAction(c *cli.Context) {
	// グローバルオプション
	/*
		var isDry = c.GlobalBool("dryrun")
		if isDry {
			fmt.Println("this is dry-run")
		}
	*/

	// パラメータ
	/*
		var paramFirst = ""
		if len(c.Args()) > 0 {
			paramFirst = c.Args().First() //c.Args()[0]と同義
		}
		fmt.Printf("Hello world! %s\n", paramFirst)
	*/

	values := spreadSheets.GetSampleValues()

	for _, row := range values {
		// Print columns A and E, which correspond to indices 0 and 4.
		fmt.Printf("%s\n", row[2])
	}
}
