package command

import (
	"context"
	"errors"
	"strings"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
)

type DatasourceConfig struct {
	Configs []map[string]interface{} `json:"datasources"`
}

func (dsc *DatasourceConfig) GetDatasoruce(query string) (datasource.Datasource, error) {
	q := strings.Split(query, "/")
	var cfg map[string]interface{}
	for i := range dsc.Configs {
		if dsc.Configs[i]["name"] == q[0] {
			cfg = dsc.Configs[i]
		}
	}
	if len(cfg) == 0 {
		return nil, errors.New("undefined " + query + " in datasource config")
	}

	var ds datasource.Datasource
	switch cfg["type"] {
	case "csv":
		csv, err := datasource.NewCSVDatasource(cfg["root_path"].(string), int(cfg["column_row_index"].(float64)))
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
		mysql, err := datasource.NewMySQLDatasource(cfg["dsn"].(string))
		if err != nil {
			return nil, err
		}
		mysql.Open()
		ds = mysql
		break
	case "spanner":
		return nil, errors.New("not support type: spanner")
	default:
		return nil, errors.New("invalid type: " + cfg["type"].(string))
	}
	return ds, nil
}

func (dsc *DatasourceConfig) GetDiff(ctx context.Context, leftQuery string, rightQuery string) (*differ.DiffColumns, *differ.DiffRows, error) {
	lds, err := dsc.GetDatasoruce(leftQuery)
	if err != nil {
		return nil, nil, err
	}
	rds, err := dsc.GetDatasoruce(rightQuery)
	if err != nil {
		return nil, nil, err
	}

	leftSchemaName := strings.Split(leftQuery, "/")[1]
	leftSchema, err := lds.GetSchema(ctx, leftSchemaName)
	if err != nil {
		return nil, nil, err
	}
	leftRows, err := lds.GetRows(ctx, leftSchema)
	if err != nil {
		return nil, nil, err
	}
	rightSchemaName := strings.Split(rightQuery, "/")[1]
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
	dRows, err := d.DiffRows(leftSchema, rightSchema, leftRows, rightRows)
	if err != nil {
		return nil, nil, err
	}
	return dColumns, dRows, nil
}
