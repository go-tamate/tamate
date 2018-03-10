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
func (c *SpreadSheetsConfig) Output(path string) (string, error) {
	if path == "" {
		path = "resources/config/spreadsheets/" + c.SpreadSheetsID + "_" + c.SheetName + ".json"
	}
	jsonBytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "", err
	}
	return "", ioutil.WriteFile(path, jsonBytes, 0644)
}
