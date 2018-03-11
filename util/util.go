package util

import (
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/Mitu217/tamate/config"
	"github.com/Mitu217/tamate/datasource"
)

// GetConfigDataSource :
func GetConfigDataSource(configPath string) (datasource.DataSource, error) {
	var config *config.BaseConfig
	r, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	if err := json.NewDecoder(r).Decode(&config); err != nil {
		return nil, err
	}

	t := strings.ToLower(config.ConfigType)
	switch t {
	case "spreadsheets":
		return getSpreadSheetsDataSource(configPath)
	case "csv":
		return getCSVDataSource(configPath)
	case "sql":
		return getSQLDataSource(configPath)
	default:
		return nil, errors.New("Not defined source type. type:" + config.ConfigType)
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
