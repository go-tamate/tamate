package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// SQLConfig :
type SQLConfig struct {
	Type         string        `json:"type"`
	Server       *ServerConfig `json:"server"`
	DatabaseName string        `json:"database_name"`
	TableName    string        `json:"table_name"`
}

// NewJSONSQLConfig :
func NewJSONSQLConfig(jsonPath string) (*SQLConfig, error) {
	var config *SQLConfig
	r, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}
	if err := json.NewDecoder(r).Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}

// Output :
func (c *SQLConfig) Output(path string) (string, error) {
	// Set default path and default file name.
	if path == "" {
		path = "resources/config/sql/" + c.Server.DriverName + "_" + c.Server.Host + "_" + c.DatabaseName + ".json"
	}
	jsonBytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "", err
	}
	return path, ioutil.WriteFile(path, jsonBytes, 0644)
}
