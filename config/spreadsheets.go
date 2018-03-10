package config

import (
	"encoding/json"
	"io/ioutil"
)

// SpreadSheetsConfig :
type SpreadSheetsConfig struct {
	SpreadSheetsID string `json:"driver_name"`
	SheetName      string `json:"sheet_name"`
	Range          string `json:"range"`
}

// Output :
func (c *SpreadSheetsConfig) Output(path string) error {
	jsonBytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, jsonBytes, 0644)
}
