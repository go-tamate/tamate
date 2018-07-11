package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
	"github.com/urfave/cli"
)

type datasourceConfig map[string]interface{}

func (c datasourceConfig) GetDatasource() (datasource.Datasource, error) {
	var ds datasource.Datasource
	switch c["type"] {
	case "csv":
		csv, err := datasource.NewCSVDatasource(c["root_path"].(string), int(c["column_row_index"].(float64)))
		if err != nil {
			return nil, err
		}
		ds = csv
		break
	case "spreadsheet":
		return nil, errors.New("not support type: spreadsheet")
		/*
			spreadsheet, err := datasource.NewSpreadsheetDatasource()
			if err != nil {
				return nil, err
			}
			ds = spreadsheet
			break
		*/
	case "mysql":
		mysql, err := datasource.NewMySQLDatasource(c["dsn"].(string))
		if err != nil {
			return nil, err
		}
		if err := mysql.Open(); err != nil {
			return nil, err
		}
		ds = mysql
		break
	case "spanner":
		return nil, errors.New("not support type: spanner")
	default:
		return nil, errors.New("invalid type: " + c["type"].(string))
	}
	return ds, nil
}

/*
 * Main
 */

func main() {
	d := &struct {
		DatasourceConfigs []datasourceConfig `json:"datasources"`
	}{}

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
		ds := c.String("datasources")
		f, err := os.Open(ds)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		if err := json.NewDecoder(f).Decode(d); err != nil {
			fmt.Println(err)
			return nil
		}
		return nil
	}
	app.Action = func(c *cli.Context) error {
		if c.NArg() < 2 {
			fmt.Println(errors.New("must specify 2 datasources"))
			return nil
		}

		lds, err := getDatasource(d.DatasourceConfigs, c.Args().Get(0))
		if err != nil {
			fmt.Println(err)
			return nil
		}
		lscn, err := getSchemaName(c.Args().Get(0))
		if err != nil {
			fmt.Println(err)
			return nil
		}
		rds, err := getDatasource(d.DatasourceConfigs, c.Args().Get(1))
		if err != nil {
			fmt.Println(err)
			return nil
		}
		rscn, err := getSchemaName(c.Args().Get(1))
		if err != nil {
			fmt.Println(err)
			return nil
		}

		ctx := context.Background()
		cols, rows, err := diff(ctx, lds, rds, lscn, rscn)
		if err != nil {
			fmt.Println(err)
			return nil
		}

		print(cols, rows)
		return nil
	}
	app.HideVersion = true // disable version flag
	app.Run(os.Args)
}

func getDatasource(datasourceConfigs []datasourceConfig, query string) (datasource.Datasource, error) {
	q := strings.Split(query, "/")
	for i := range datasourceConfigs {
		if datasourceConfigs[i]["label"] == q[0] {
			return datasourceConfigs[i].GetDatasource()
		}
	}
	return nil, errors.New("undefined " + query + " in datasource config")
}

func getSchemaName(query string) (string, error) {
	q := strings.Split(query, "/")
	if len(q) < 2 {
		return "", fmt.Errorf("%+v is invalid query", query)
	}
	return q[1], nil
}

/*
 * Actions
 */

func diff(ctx context.Context, lds, rds datasource.Datasource, leftSchemaName, rightSchemaName string) (*differ.DiffColumns, *differ.DiffRows, error) {
	leftSchema, err := lds.GetSchema(ctx, leftSchemaName)
	if err != nil {
		return nil, nil, err
	}
	leftRows, err := lds.GetRows(ctx, leftSchema)
	if err != nil {
		return nil, nil, err
	}
	rightSchema, err := rds.GetSchema(ctx, rightSchemaName)
	if err != nil {
		return nil, nil, err
	}
	rightRows, err := rds.GetRows(ctx, rightSchema)
	if err != nil {
		return nil, nil, err
	}

	d, err := differ.NewDiffer()
	if err != nil {
		return nil, nil, err
	}
	dColumns, err := d.DiffColumns(leftSchema, rightSchema)
	if err != nil {
		return nil, nil, err
	}
	dRows, err := d.DiffRows(leftSchema, leftRows, rightRows)
	if err != nil {
		return nil, nil, err
	}
	return dColumns, dRows, nil
}

func print(diffColumns *differ.DiffColumns, diffRows *differ.DiffRows) {
	fmt.Println("=== Columns ===")
	if len(diffColumns.Left) > 0 || len(diffColumns.Right) > 0 {
		for i := range diffColumns.Left {
			fmt.Println("- " + diffColumns.Left[i].String())
		}
		for i := range diffColumns.Right {
			fmt.Println("+ " + diffColumns.Right[i].String())
		}
	} else {
		fmt.Println("empty diff")
	}
	fmt.Println("")
	fmt.Println("=== Rows ===")
	if len(diffRows.Left) > 0 || len(diffRows.Right) > 0 {
		for i := range diffRows.Left {
			fmt.Println("- " + diffRows.Left[i].String())
		}
		for i := range diffRows.Right {
			fmt.Println("+ " + diffRows.Right[i].String())
		}
	} else {
		fmt.Println("empty diff")
	}
}
