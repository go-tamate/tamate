package main

import (
	"fmt"
	"os"

	"github.com/Mitu217/tamate/dumper"

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
			Name:   "dump:spreadsheets",
			Usage:  "Dump CSV from SpreadSheets.",
			Action: dumpSpreadSheetsAction,
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

func dumpSpreadSheetsAction(c *cli.Context) {
	// Check args.
	if len(c.Args()) < 2 {
		fmt.Println("[Error] Argument is missing! 2 arguments are required.")
	}

	outputPath := c.Args()[1]

	sheetsID := "1uCEt_DpNCRPZjvxS0hdnIhSnQQKYjmV0FN2KneRbkKk" //c.Args()[0]
	sheetName := "Class Data"
	targetRange := "A1:XX"

	sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
	if err != nil {
		panic(err)
	}

	sheetConfig := datasource.NewSpreadSheetsConfig(sheetsID, sheetName, targetRange)
	sheetDataSource, err := datasource.NewSpreadSheetsDataSource(sc, sheetConfig)
	if err != nil {
		panic(err)
	}

	csvConfig := datasource.NewCSVConfig("", outputPath)
	csvDataSource, err := datasource.NewCSVDataSource(sc, csvConfig)
	if err != nil {
		panic(err)
	}

	// dump rows by dumper
	d := dumper.NewDumper()
	d.Dump(sheetDataSource, csvDataSource)
}

func dumpSQLAction(c *cli.Context) {
	// Check args.
	if len(c.Args()) < 4 {
		fmt.Println("[Error] Argument is missing! 4 arguments are required.")
	}

	/*
		hostSettingPath := c.Args()[0]
		outputPath := c.Args()[1]
		dbName := c.Args()[2]
		tableName := c.Args()[3]

		sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
		server, err := server.NewJsonFileServer(hostSettingPath)
		if err != nil {
			panic(err)
		}
		ds := &datasource.SQLDataSource{
			Server:       server,
			DatabaseName: dbName,
			TableName:    tableName,
		}
		if err = ds.Dump(sc); err != nil {
			panic(err)
		}
		ds.OutputCSV(sc, outputPath, ds.Columns, ds.Values)
	*/
}
