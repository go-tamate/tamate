package config

import (
	"encoding/json"
	"os"
)

// SQLConfig :
type SQLConfig struct {
	ConfigType   string        `json:"config_type"`
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
