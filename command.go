package main

import (
	"fmt"
	"os"

	"github.com/Mitu217/tamate/differ"
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
		{
			Name:   "diff",
			Usage:  "Diff Sample.",
			Action: diffAction,
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

	sc, err := schema.NewJSONFileSchema("./resources/schema/sample.json")
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

	hostSettingPath := c.Args()[0]
	outputPath := c.Args()[1]
	dbName := c.Args()[2]
	tableName := c.Args()[3]

	sc, err := schema.NewJSONFileSchema("./resources/schema/sample.json")
	if err != nil {
		panic(err)
	}

	sqlConfig, err := datasource.NewJSONSQLConfig(hostSettingPath, dbName, tableName)
	if err != nil {
		panic(err)
	}
	sqlDataSource, err := datasource.NewSQLDataSource(sc, sqlConfig)
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
	d.Dump(sqlDataSource, csvDataSource)
}

func diffAction(c *cli.Context) {
	sc, err := schema.NewJSONFileSchema("./resources/schema/sample.json")
	if err != nil {
		panic(err)
	}

	// spreaddsheets
	sheetsID := "1uCEt_DpNCRPZjvxS0hdnIhSnQQKYjmV0FN2KneRbkKk" //c.Args()[0]
	sheetName := "Class Data"
	targetRange := "A1:XX"

	sheetConfig := datasource.NewSpreadSheetsConfig(sheetsID, sheetName, targetRange)
	sheetDataSource, err := datasource.NewSpreadSheetsDataSource(sc, sheetConfig)
	if err != nil {
		panic(err)
	}

	// sql
	hostSettingPath := "./resources/host/mysql/sample.json"
	dbName := "Sample"
	tableName := "Sample"

	sqlConfig, err := datasource.NewJSONSQLConfig(hostSettingPath, dbName, tableName)
	if err != nil {
		panic(err)
	}
	sqlDataSource, err := datasource.NewSQLDataSource(sc, sqlConfig)
	if err != nil {
		panic(err)
	}

	// dump rows by dumper
	d := differ.NewDiffer(sheetDataSource, sqlDataSource)
	//diff, err := d.RowsOnlyLeft(sc)
	diff, err := d.RowsOnlyRight(sc)

	fmt.Println("[Add]")
	for _, add := range diff.Add.Values {
		fmt.Println(add)
	}
	fmt.Println("[Delete]")
	for _, delete := range diff.Delete.Values {
		fmt.Println(delete)
	}
	fmt.Println("[Modify]")
	for _, modify := range diff.Modify.Values {
		fmt.Println(modify)
	}
}
