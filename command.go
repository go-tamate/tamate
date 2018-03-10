package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"

	"github.com/Mitu217/tamate/config"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/Mitu217/tamate/differ"

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

func generateConfig(configType string, outputPath string) (string, error) {
	switch configType {
	case "SQL":
		config := config.ServerConfig{}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("DriverName: ")
			fmt.Scan(&config.DriverName)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("Host: ")
			fmt.Scan(&config.Host)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("Port: ")
			fmt.Scan(&config.Port)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("User: ")
			fmt.Scan(&config.User)
		}
		if terminal.IsTerminal(syscall.Stdin) {
			fmt.Print("Password: ")
			fmt.Scan(&config.Password)
		}
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

func generateSchemaAction(c *cli.Context) {
	if len(c.Args()) < 1 || c.Args()[0] == "" {
		log.Fatalln("Please specify the output path.")
	}
	outputPath := c.Args()[0]

	inputType := c.String("type")

	switch inputType {
	case "SQL":
		// FIXME: 対話式に情報を収集した方がよさそう
		driverName := c.Args()[1]
		host := c.Args()[2]
		port, _ := strconv.Atoi(c.Args()[3])
		user := c.Args()[4]
		pw := c.Args()[5]
		dbName := c.Args()[6]
		tableName := c.Args()[7]
		server := &schema.Server{
			DriverName: driverName,
			Host:       host,
			Port:       port,
			User:       user,
			Password:   pw,
		}
		sc := &schema.SQLSchema{
			Server:       server,
			DatabaseName: dbName,
		}
		sc.NewServerSchema(tableName)
		sc.Output(outputPath)
		break
	case "SpreadSheets":

		break
	case "CSV":
		break
	default:
		log.Fatalln("Not defined input type. type:" + inputType)
	}
}

func dumpAction(c *cli.Context) {
	/*
		inputDatasourceType := c.String("input")
		outputDatasourceType := c.String("output")

		// read schema
		if len(c.Args()) >= 1 || c.Args()[0] == "" {
			log.Fatalf("Please specify the schema file path.")
			return
		}
		schemaFilePath := c.Args()[0]
		sc, err := schema.NewJSONFileSchema(schemaFilePath)
		if err != nil {
			log.Fatalf("Unable to read schema file: %v", err)
		}

		// create input datasource
		if len(c.Args()) >= 2 || c.Args()[1] == "" {
			log.Fatalf("Please specify the input datasource config path.")
			return
		}
		inputConfigPath := c.Args()[1]
		inputDatasource, err := getDatasource(sc, inputDatasourceType, inputConfigPath)
	*/
}

func diffAction(c *cli.Context) {
	sc, err := schema.NewJSONFileSchema("./resources/schema/sample.json")
	if err != nil {
		log.Fatalf("Unable to read schema file: %v", err)
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
	d, err := differ.NewSchemaDiffer(sc, sheetDataSource, sqlDataSource)
	if err != nil {
		panic(err)
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

func getDatasource(sc schema.Schema, sourceType string, configPath string) (datasource.DataSource, error) {
	switch sourceType {
	case "SpreadSheets":
		return getSpreadSheetsDataSource(sc)
	case "CSV":
		return nil, nil
	case "SQL":
		return nil, nil
	default:
		return nil, errors.New("Not defined source type. type:" + sourceType)
	}
}

func getSpreadSheetsDataSource(sc schema.Schema) (*datasource.SpreadSheetsDataSource, error) {
	/*
		sheetsID := "1uCEt_DpNCRPZjvxS0hdnIhSnQQKYjmV0FN2KneRbkKk" //c.Args()[0]
		sheetName := "Class Data"
		targetRange := "A1:XX"

		sheetConfig := datasource.NewSpreadSheetsConfig(sheetsID, sheetName, targetRange)
		sheetDataSource, err := datasource.NewSpreadSheetsDataSource(sc, sheetConfig)
		if err != nil {
			panic(err)
		}
	*/
	return nil, nil
}

/*
func dumpSpreadSheetsAction(c *cli.Context) {


	outputPath := c.Args()[1]



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
*/
