package util

import (
	"errors"
	"strings"

	"github.com/Mitu217/tamate/datasource"
)

// GetConfigDataSource :
func GetConfigDataSource(configPath string) (datasource.DataSource, error) {
	var conf struct{Type string `json:"type"`}
	if err := datasource.NewConfigFromJSONFile(configPath, &conf); err != nil {
		return nil, err
	}

	t := strings.ToLower(conf.Type)
	switch t {
	case "spreadsheets":
		return getSpreadSheetsDataSource(configPath)
	case "csv":
		return getCSVDataSource(configPath)
	case "sql":
		return getSQLDataSource(configPath)
	default:
		return nil, errors.New("Not defined source type. type:" + conf.Type)
	}
}

func getSpreadSheetsDataSource(configPath string) (*datasource.SpreadSheetsDataSource, error) {
	conf := &datasource.SpreadSheetsDatasourceConfig{}
	if err := datasource.NewConfigFromJSONFile(configPath, conf); err != nil {
		return nil, err
	}
	ds, err := datasource.NewSpreadSheetsDataSource(conf)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func getCSVDataSource(configPath string) (*datasource.CSVDataSource, error) {
	conf := &datasource.CSVDatasourceConfig{}
	if err := datasource.NewConfigFromJSONFile(configPath, conf); err != nil {
		return nil, err
	}
	ds, err := datasource.NewCSVDataSource(conf)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func getSQLDataSource(configPath string) (*datasource.SQLDataSource, error) {
	conf := &datasource.SQLDatasourceConfig{}
	if err := datasource.NewConfigFromJSONFile(configPath, conf); err != nil {
		return nil, err
	}
	ds, err := datasource.NewSQLDataSource(conf)
	if err != nil {
		return nil, err
	}
	return ds, nil
}
