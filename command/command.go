package main

import (
	"fmt"
	"os"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/schema"
	"github.com/Mitu217/tamate/server"

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
			Name:   "dump:sql",
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
		fmt.Println("[Error] Argument is missing! 2 arguments are required.")
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
	// Check args.
	if len(c.Args()) < 4 {
		fmt.Println("[Error] Argument is missing! 4 arguments are required.")
	}

	hostSettingPath := c.Args()[0]
	dbName := c.Args()[1]
	//tableName := c.Args()[2]
	outputPath := c.Args()[3]

	sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
	server, err := server.NewJsonFileServer(hostSettingPath)
	if err != nil {
		panic(err)
	}
	ds := &datasource.SQLDatabase{
		Server:       server,
		DatabaseName: dbName,
	}
	if err = ds.Dump(sc); err != nil {
		panic(err)
	}

	for _, table := range ds.Tables {
		ds.OutputCSV(sc, outputPath, table.Columns, table.Records)
	}
}
