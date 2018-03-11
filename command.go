package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"syscall"

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
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "schema, s",
					Usage: "schema file path.",
				},
				cli.StringFlag{
					Name:  "input, i",
					Usage: "input DataSource type.",
				},
				cli.StringFlag{
					Name:  "output, o",
					Usage: "input DataSource type. If not specified, standard output",
				},
			},
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
				cli.StringFlag{
					Name:  "left, l",
					Usage: "left DataSource type.",
				},
				cli.StringFlag{
					Name:  "right, r",
					Usage: "right DataSource type.",
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

	inputType := c.String("type")
	configPath := c.String("config")

	switch inputType {
	case "SQL":
		if configPath == "" {
			path, err := generateConfig(inputType, outputPath)
			if err != nil {
				log.Fatalln(err)
			}
			configPath = path
		}
		config, err := config.NewJSONSQLConfig(configPath)
		if err != nil {
			log.Fatalf("Unable to read config file: %v", err)
		}
		ds, err := datasource.NewSQLDataSource(config)
		if err != nil {
			log.Fatalln(err)
		}
		sc, err := ds.GetSchema()
		if err != nil {
			log.Fatalln(err)
		}
		if err := sc.Output(outputPath); err != nil {
			log.Fatalln(err)
		}
		break
	default:
		log.Fatalln("Not defined input type. type:" + inputType)
	}
}

func dumpAction(c *cli.Context) {
	inputDatasourceType := c.String("input")
	outputDatasourceType := c.String("output")

	// input datasource
	if len(c.Args()) < 1 || inputDatasourceType == "" {
		log.Fatalf("Please specify the input type and datasource config path.")
	}
	inputConfigPath := c.Args()[0]
	inputDatasource, err := getDatasource(inputDatasourceType, inputConfigPath)
	if err != nil {
		log.Fatalln(err)
	}

	// output datasource
	if outputDatasourceType == "" {
		// standard output
		rows, err := inputDatasource.GetRows()
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(rows)
	} else {
		if len(c.Args()) < 2 {
			log.Fatalf("Please specify the output datasource config path.")
		}
		outputConfigPath := c.Args()[1]
		outputDatasource, err := getDatasource(outputDatasourceType, outputConfigPath)
		if err != nil {
			log.Fatalln(err)
		}
		sc, err := outputDatasource.GetSchema()
		if err != nil {
			log.Fatalln(err)
		}
		if err := inputDatasource.SetSchema(sc); err != nil {
			log.Fatalln(err)
		}

		d := dumper.NewDumper()
		if err := d.Dump(inputDatasource, outputDatasource); err != nil {
			log.Fatalln(err)
		}
	}
}

func diffAction(c *cli.Context) {
	schemaFilePath := c.String("schema")
	leftDatasourceType := c.String("left")
	rightDatasourceType := c.String("right")

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
	if len(c.Args()) < 1 || leftDatasourceType == "" {
		log.Fatalf("Please specify the left type and datasource config path.")
	}
	leftConfigPath := c.Args()[0]
	leftDatasource, err := getDatasource(leftDatasourceType, leftConfigPath)
	if err != nil {
		log.Fatalln(err)
	}
	if err := leftDatasource.SetSchema(diffSchema); err != nil {
		log.Fatalln(err)
	}

	// right datasource
	if len(c.Args()) < 2 || rightDatasourceType == "" {
		log.Fatalf("Please specify the right type and datasource config path.")
	}
	rightConfigPath := c.Args()[1]
	rightDatasource, err := getDatasource(rightDatasourceType, rightConfigPath)
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
	fmt.Println(diff.Add.Columns)
	for _, add := range diff.Add.Values {
		fmt.Println(add)
	}
	fmt.Println("[Delete]")
	fmt.Println(diff.Delete.Columns)
	for _, delete := range diff.Delete.Values {
		fmt.Println(delete)
	}
	fmt.Println("[Modify]")
	fmt.Println(diff.Modify.Columns)
	for _, modify := range diff.Modify.Values {
		fmt.Println(modify)
	}
}

func generateConfig(configType string, outputPath string) (string, error) {
	switch configType {
	case "SQL":
		server := config.ServerConfig{}
		config := config.SQLConfig{}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("DriverName: ")
			fmt.Scan(&server.DriverName)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("Host: ")
			fmt.Scan(&server.Host)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("Port: ")
			fmt.Scan(&server.Port)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("User: ")
			fmt.Scan(&server.User)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("Password: ")
			fmt.Scan(&server.Password)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("DatabaseName: ")
			fmt.Scan(&config.DatabaseName)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("TableName: ")
			fmt.Scan(&config.TableName)
		}
		config.Server = &server
		return config.Output(outputPath)
	case "SpreadSheets":
		config := config.SpreadSheetsConfig{}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("SpreadSheetsID: ")
			fmt.Scan(&config.SpreadSheetsID)
		}
		// TODO: スペース入りの文字列が対応不可
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("SheetName: ")
			fmt.Scan(&config.SheetName)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("Range: ")
			fmt.Scan(&config.Range)
		}
		return config.Output(outputPath)
	case "CSV":
		config := config.CSVConfig{}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("FilePath: ")
			fmt.Scan(&config.Path)
		}
		return config.Output(outputPath)
	default:
		return "", errors.New("Not defined input type. type:" + configType)
	}
}

func getDatasource(sourceType string, configPath string) (datasource.DataSource, error) {
	switch sourceType {
	case "SpreadSheets":
		return getSpreadSheetsDataSource(configPath)
	case "CSV":
		return getCSVDataSource(configPath)
	case "SQL":
		return getSQLDataSource(configPath)
	default:
		return nil, errors.New("Not defined source type. type:" + sourceType)
	}
}

func getSpreadSheetsDataSource(configPath string) (*datasource.SpreadSheetsDataSource, error) {
	config, err := config.NewJSONSpreadSheetsConfig(configPath)
	if err != nil {
		return nil, err
	}
	ds, err := datasource.NewSpreadSheetsDataSource(config)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func getCSVDataSource(configPath string) (*datasource.CSVDataSource, error) {
	config, err := config.NewJSONCSVConfig(configPath)
	if err != nil {
		return nil, err
	}
	ds, err := datasource.NewCSVDataSource(config)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func getSQLDataSource(configPath string) (*datasource.SQLDataSource, error) {
	config, err := config.NewJSONSQLConfig(configPath)
	if err != nil {
		return nil, err
	}
	ds, err := datasource.NewSQLDataSource(config)
	if err != nil {
		return nil, err
	}
	return ds, nil
}
