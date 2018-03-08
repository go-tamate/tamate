package main

import (
	"fmt"
	"os"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/schema"

	"github.com/codegangsta/cli"
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
			Name:   "dump:spreadsheet",
			Usage:  "Dump CSV from SpreadSheet.",
			Action: dumpSpreadSheetAction,
		},
		{
			Name:   "dump:sql, dsql",
			Usage:  "Dump CSV from SQL Server.",
			Action: dumpSQLAction,
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

func dumpSpreadSheetAction(c *cli.Context) {
	// Check args.
	if len(c.Args()) < 2 {
		fmt.Println("[Error] Argument is missing! 3 arguments are required.")
	}

	spreadSheetsID := c.Args()[0]
	outputPath := c.Args()[1]

	sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
	if err != nil {
		panic(err)
	}
	ds := datasource.SpreadSheetsDataSource{
		SpreadSheetsID: spreadSheetsID,
	}
	if err = ds.OutputCSV(sc, outputPath); err != nil {
		panic(err)
	}
}

func dumpSQLAction(c *cli.Context) {
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

}
