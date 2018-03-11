package config

import (
	"encoding/json"
	"os"
)

// CSVConfig :
type CSVConfig struct {
	ConfigType string `json:"config_type"`
	Path       string `json:"path"`
}

// NewJSONCSVConfig :
func NewJSONCSVConfig(jsonPath string) (*CSVConfig, error) {
	var config *CSVConfig
	r, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r).Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}
