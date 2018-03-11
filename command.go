package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/Mitu217/tamate/util"

	"github.com/Mitu217/tamate/differ"
	"github.com/Mitu217/tamate/dumper"

	"github.com/Mitu217/tamate/config"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/schema"

	"github.com/urfave/cli"
)

var leftSource string
var rightSource string

func main() {
	app := cli.NewApp()
	app.Name = "tamate"
	app.Usage = "tamate commands"
	app.Version = "0.1.0"

	// Commands
	app.Commands = []cli.Command{
		{
			Name:   "generate:config",
			Usage:  "Generate config file.",
			Action: generateConfigAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "type, t",
					Usage: "",
				},
				cli.StringFlag{
					Name:  "output, o",
					Usage: "",
				},
			},
		},
		{
			Name:   "generate:schema",
			Usage:  "Generate schema file.",
			Action: generateSchemaAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "type, t",
					Usage: "",
				},
				cli.StringFlag{
					Name:  "config, c",
					Usage: "",
				},
				cli.StringFlag{
					Name:  "output, o",
					Usage: "",
				},
			},
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
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "schema, s",
					Usage: "schema file path.",
				},
			},
		},
	}

	// Run Commands
	app.Run(os.Args)
}

func generateConfigAction(c *cli.Context) {
	// Override output path
	outputPath := ""
	if c.String("output") != "" {
		outputPath = c.String("output")
	}

	_, err := generateConfig(c.String("type"), outputPath)
	if err != nil {
		log.Fatalln(err)
	}
}

func generateSchemaAction(c *cli.Context) {
	// Override output path
	outputPath := ""
	if c.String("output") != "" {
		outputPath = c.String("output")
	}

	inputType := strings.ToLower(c.String("type"))
	configPath := c.String("config")

	switch inputType {
	case "sql":
		if configPath == "" {
			path, err := generateConfig(inputType, outputPath)
			if err != nil {
				log.Fatalln(err)
			}
			configPath = path
		}
		conf := &datasource.SQLDatasourceConfig{}
		if err := datasource.NewConfigFromJSONFile(configPath, conf); err != nil {
			log.Fatalf("Unable to read config file: %v", err)
		}
		ds, err := datasource.NewSQLDataSource(conf)
		if err != nil {
			log.Fatalln(err)
		}
		sc, err := ds.GetSchema()
		if err != nil {
			log.Fatalln(err)
		}
		_, err = sc.OutputJSON(outputPath)
		if err != nil {
			log.Fatalln(err)
		}
		break
	default:
		log.Fatalln("Not defined input type. type:" + inputType)
	}
}

func dumpAction(c *cli.Context) {
	// Get InputDataSource
	if len(c.Args()) < 1 {
		log.Fatalf("Please specify the input type and datasource config path.")
	}
	inputConfigPath := c.Args()[0]
	inputDatasource, err := util.GetConfigDataSource(inputConfigPath)
	if err != nil {
		log.Fatalln(err)
	}

	if len(c.Args()) < 2 {
		// Standard Output
		rows, err := inputDatasource.GetRows()
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("[Input DataSource]")
		printRows(rows)
	} else {
		// Get OutputDataSource
		outputConfigPath := c.Args()[1]
		outputDatasource, err := util.GetConfigDataSource(outputConfigPath)
		if err != nil {
			log.Fatalln(err)
		}

		// Dump
		sc, err := outputDatasource.GetSchema()
		if err != nil {
			// Schemaの取得に失敗
			log.Fatalln(err)
		}
		if err := inputDatasource.SetSchema(sc); err != nil {
			// dump元のSchema設定に失敗
			log.Fatalln(err)
		}
		d := dumper.NewDumper()
		if err := d.Dump(inputDatasource, outputDatasource); err != nil {
			// Dumpに失敗
			log.Fatalln(err)
		}
	}
}

func diffAction(c *cli.Context) {
	schemaFilePath := c.String("schema")

	// read schema
	var diffSchema *schema.Schema
	if schemaFilePath != "" {
		sc, err := schema.NewJSONFileSchema(schemaFilePath)
		if err != nil {
			log.Fatalf("Unable to read schema file: %v", err)
		}
		diffSchema = sc
	}

	// left datasource
	if len(c.Args()) < 1 {
		log.Fatalf("Please specify the left type and datasource config path.")
	}
	leftConfigPath := c.Args()[0]
	leftDatasource, err := util.GetConfigDataSource(leftConfigPath)
	if err != nil {
		log.Fatalln(err)
	}
	if err := leftDatasource.SetSchema(diffSchema); err != nil {
		log.Fatalln(err)
	}

	// right datasource
	if len(c.Args()) < 2 {
		log.Fatalf("Please specify the right type and datasource config path.")
	}
	rightConfigPath := c.Args()[1]
	rightDatasource, err := util.GetConfigDataSource(rightConfigPath)
	if err != nil {
		log.Fatalln(err)
	}
	if err := rightDatasource.SetSchema(diffSchema); err != nil {
		log.Fatalln(err)
	}

	var d *differ.Differ
	if diffSchema == nil {
		rowsDiffer, err := differ.NewRowsDiffer(leftDatasource, rightDatasource)
		if err != nil {
			log.Fatalln(err)
		}
		d = rowsDiffer
	} else {
		schemaDiffer, err := differ.NewSchemaDiffer(diffSchema, leftDatasource, rightDatasource)
		if err != nil {
			log.Fatalln(err)
		}
		d = schemaDiffer
	}
	if err != nil {
		log.Fatalln(err)
	}
	diff, err := d.DiffRows()
	fmt.Println("[Add]")
	printRows(diff.Add)
	fmt.Println("[Delete]")
	printRows(diff.Delete)
	fmt.Println("[Modify]")
	printRows(diff.Modify)
}

func generateConfig(configType string, outputPath string) (string, error) {
	t := strings.ToLower(configType)
	isStdinTerm := terminal.IsTerminal(0) // fd0: stdin
	switch t {
	case "sql":
		server := config.ServerConfig{}
		c := config.SQLConfig{ConfigType: t}
		if isStdinTerm {
			fmt.Print("DriverName: ")
			fmt.Scan(&server.DriverName)
		}
		if isStdinTerm {
			fmt.Print("Host: ")
			fmt.Scan(&server.Host)
		}
		if isStdinTerm {
			fmt.Print("Port: ")
			fmt.Scan(&server.Port)
		}
		if isStdinTerm {
			fmt.Print("User: ")
			fmt.Scan(&server.User)
		}
		if isStdinTerm {
			fmt.Print("Password: ")
			fmt.Scan(&server.Password)
		}
		if isStdinTerm {
			fmt.Print("DatabaseName: ")
			fmt.Scan(&c.DatabaseName)
		}
		if isStdinTerm {
			fmt.Print("TableName: ")
			fmt.Scan(&c.TableName)
		}
		c.Server = &server
		return config.OutputJSON(c, outputPath)
	case "spreadsheets":
		c := config.SpreadSheetsConfig{ConfigType: t}
		if isStdinTerm {
			fmt.Print("SpreadSheetsID: ")
			fmt.Scan(&c.SpreadSheetsID)
		}
		// TODO: スペース入りの文字列が対応不可
		if isStdinTerm {
			fmt.Print("SheetName: ")
			fmt.Scan(&c.SheetName)
		}
		if isStdinTerm {
			fmt.Print("Range: ")
			fmt.Scan(&c.Range)
		}
		return config.OutputJSON(c, outputPath)
	case "csv":
		c := config.CSVConfig{ConfigType: t}
		if isStdinTerm {
			fmt.Print("FilePath: ")
			fmt.Scan(&c.Path)
		}
		return config.OutputJSON(c, outputPath)
	default:
		return "", errors.New("Not defined input type. type:" + configType)
	}
}

func printRows(rows *datasource.Rows) {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', tabwriter.TabIndent)
	// Columns
	w.Write([]byte(strings.Join(rows.Columns, "\t") + "\n"))
	// Values
	for i := range rows.Values {
		w.Write([]byte(strings.Join(rows.Values[i], "\t") + "\n"))
	}
	w.Flush()
}
