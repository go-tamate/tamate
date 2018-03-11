package config

import (
	"encoding/json"
	"os"
)

// SpreadSheetsConfig :
type SpreadSheetsConfig struct {
	ConfigType     string `json:"config_type"`
	SpreadSheetsID string `json:"spreadsheets_id"`
	SheetName      string `json:"sheet_name"`
	Range          string `json:"range"`
}

// NewJSONSpreadSheetsConfig :
func NewJSONSpreadSheetsConfig(jsonPath string) (*SpreadSheetsConfig, error) {
	var config *SpreadSheetsConfig
	r, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r).Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}
