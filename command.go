package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Mitu217/tamate/differ"

	"github.com/Mitu217/tamate/table"

	"encoding/json"
	"github.com/Mitu217/tamate/table/schema"
	"github.com/urfave/cli"
	"io"
	"text/tabwriter"
)

func main() {
	app := cli.NewApp()
	app.Name = "tamate"
	app.Usage = "tamate commands"
	app.Version = "0.1.0"

	// Commands
	app.Commands = []cli.Command{
		{
			Name:   "generate:table",
			Usage:  "generate:table <table=(csv|spreadsheet|sql)>",
			Action: generateTableAction,
		},
		{
			Name:   "dump",
			Usage:  "Dump Command.",
			Action: dumpAction,
		},
		{
			Name:   "diff",
			Usage:  "Diff between 2 schema.",
			Action: diffAction,
		},
	}

	// Run Commands
	if err := app.Run(os.Args); err != nil {
		log.Fatalln(err)
	}
}

func generateTableAction(c *cli.Context) {
	if c.NArg() < 1 {
		log.Fatalf("please specify table type (csv, spreadsheet or sql)")
	}
	if err := generateTable(os.Stdout, c.Args()[0]); err != nil {
		log.Fatalln(err)
	}
}

func dumpAction(c *cli.Context) {
	// Get InputDataSource
	if len(c.Args()) < 1 {
		log.Fatalf("Please specify the input type and datasource config path.")
	}
	tablePath := c.Args()[0]
	r, err := os.Open(tablePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer r.Close()
	tbl, err := table.FromJSON(r)
	if err != nil {
		log.Fatalln(err)
	}

	rows, err := tbl.GetRows()
	if err != nil {
		log.Fatalln(err)
	}

	sc, err := tbl.GetSchema()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("[Input DataSource]")
	printRows(sc, rows)
}

func diffAction(c *cli.Context) {
	if len(c.Args()) < 2 {
		log.Fatalf("Please specify the two tables")
	}

	leftTablePath := c.Args()[0]
	fl, err := os.Open(leftTablePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer fl.Close()

	leftTable, err := table.FromJSON(fl)
	if err != nil {
		log.Fatalln(err)
	}

	rightTablePath := c.Args()[1]
	fr, err := os.Open(rightTablePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer fr.Close()

	rightTable, err := table.FromJSON(fr)
	if err != nil {
		log.Fatalln(err)
	}

	var d *differ.Differ
	rowsDiffer, err := differ.NewRowsDiffer(leftTable, rightTable)
	if err != nil {
		log.Fatalln(err)
	}
	d = rowsDiffer
	if err != nil {
		log.Fatalln(err)
	}
	diff, err := d.DiffRows()
	if err != nil {
		log.Fatalln(err)
	}

	sc, err := leftTable.GetSchema()
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("[Add]")
	printRows(sc, diff.Add)
	fmt.Println("[Delete]")
	printRows(sc, diff.Delete)
	fmt.Println("[Modify]")
	printRows(sc, diff.Modify)
}

func generateTable(w io.Writer, configType string) error {
	t := strings.ToLower(configType)
	enc := json.NewEncoder(w)

	switch t {
	case "sql":
		conf := &table.SQLTable{Schema: &schema.Schema{}, Config: &table.SQLTableConfig{}}
		return enc.Encode(conf)
	case "spreadsheet":
		conf := &table.SpreadsheetTable{Schema: &schema.Schema{}, Config: &table.SpreadsheetTableConfig{}}
		return enc.Encode(conf)
	case "csv":
		conf := &table.CSVTable{Schema: &schema.Schema{}, Config: &table.CSVConfig{}}
		return enc.Encode(conf)
	default:
		return errors.New("Not defined input type. type:" + configType)
	}
}

func printRows(sc *schema.Schema, rows *table.Rows) {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', tabwriter.TabIndent)
	w.Write([]byte(strings.Join(sc.ColumnNames(), "\t") + "\n"))

	for i := range rows.Values {
		w.Write([]byte(strings.Join(rows.Values[i], "\t") + "\n"))
	}
	w.Flush()
}
